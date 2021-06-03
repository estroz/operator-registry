package action

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/operator-framework/operator-registry/internal/declcfg"
	"github.com/operator-framework/operator-registry/internal/property"
	"github.com/operator-framework/operator-registry/pkg/containertools"
	"github.com/operator-framework/operator-registry/pkg/image"
	"github.com/operator-framework/operator-registry/pkg/image/containerdregistry"
	"github.com/operator-framework/operator-registry/pkg/lib/bundle"
	"github.com/operator-framework/operator-registry/pkg/registry"
	"github.com/operator-framework/operator-registry/pkg/sqlite"
)

type Render struct {
	Refs     []string
	Registry image.Registry
}

func nullLogger() *logrus.Entry {
	logger := logrus.New()
	logger.SetOutput(ioutil.Discard)
	return logrus.NewEntry(logger)
}

func (r Render) Run(ctx context.Context) (*declcfg.DeclarativeConfig, error) {
	idx := declcfg.NewPackageIndex()
	defer func() {
		if err := idx.Cleanup(); err != nil {
			logrus.Error(err)
		}
	}()
	if err := r.index(ctx, idx); err != nil {
		return nil, err
	}

	var cfgs []declcfg.DeclarativeConfig
	for _, pkgName := range idx.GetPackageNames() {
		cfg, err := idx.LoadPackageConfig(pkgName)
		if err != nil {
			return nil, err
		}
		cfgs = append(cfgs, *cfg)
	}

	return combineConfigs(cfgs), nil
}

func (r Render) index(ctx context.Context, idx *declcfg.PackageIndex) error {
	if r.Registry == nil {
		reg, err := r.createRegistry()
		if err != nil {
			return fmt.Errorf("create registry: %v", err)
		}
		defer reg.Destroy()
		r.Registry = reg
	}

	for _, ref := range r.Refs {
		var err error
		if stat, serr := os.Stat(ref); serr == nil && stat.IsDir() {
			err = idx.IndexDir(ref)
			// cfg, err = declcfg.LoadDir(ref)
		} else {
			err = r.indexImage(ctx, idx, ref)
		}
		if err != nil {
			return fmt.Errorf("render reference %q: %v", ref, err)
		}
	}

	return nil
}

func (r Render) createRegistry() (*containerdregistry.Registry, error) {
	cacheDir, err := os.MkdirTemp("", "render-registry-")
	if err != nil {
		return nil, fmt.Errorf("create tempdir: %v", err)
	}

	reg, err := containerdregistry.NewRegistry(
		containerdregistry.WithCacheDir(cacheDir),

		// The containerd registry impl is somewhat verbose, even on the happy path,
		// so discard all logger logs. Any important failures will be returned from
		// registry methods and eventually logged as fatal errors.
		containerdregistry.WithLog(nullLogger()),
	)
	if err != nil {
		return nil, err
	}
	return reg, nil
}

func (r Render) indexImage(ctx context.Context, idx *declcfg.PackageIndex, imageRef string) error {
	ref := image.SimpleReference(imageRef)
	if err := r.Registry.Pull(ctx, ref); err != nil {
		return err
	}
	labels, err := r.Registry.Labels(ctx, ref)
	if err != nil {
		return err
	}
	tmpDir, err := ioutil.TempDir("", "render-unpack-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)
	if err := r.Registry.Unpack(ctx, ref, tmpDir); err != nil {
		return err
	}

	if dbFile, ok := labels[containertools.DbLocationLabel]; ok {
		if err = addSQLiteToPackageIndex(ctx, filepath.Join(tmpDir, dbFile), idx); err != nil {
			return err
		}
	} else if configsDir, ok := labels["operators.operatorframework.io.index.configs.v1"]; ok {
		// TODO(joelanford): Make a constant for above configs location label
		tmpConfigsDir := filepath.Join(tmpDir, configsDir)
		// TODO: does renderBundleObjects need to be called on these configs?
		if err = idx.IndexDir(tmpConfigsDir); err != nil {
			return err
		}
	} else if _, ok := labels[bundle.PackageLabel]; ok {
		img, err := registry.NewImageInput(ref, tmpDir)
		if err != nil {
			return err
		}

		cfg, err := bundleToDeclcfg(img.Bundle)
		if err != nil {
			return err
		}
		renderBundleObjects(cfg)
		if err := idx.Add(cfg); err != nil {
			return err
		}
	} else {
		labelKeys := sets.StringKeySet(labels)
		labelVals := []string{}
		for _, k := range labelKeys.List() {
			labelVals = append(labelVals, fmt.Sprintf("  %s=%s", k, labels[k]))
		}
		if len(labelVals) > 0 {
			return fmt.Errorf("render %q: image type could not be determined, found labels\n%s", ref, strings.Join(labelVals, "\n"))
		} else {
			return fmt.Errorf("render %q: image type could not be determined: image has no labels", ref)
		}
	}
	return nil
}

func addSQLiteToPackageIndex(ctx context.Context, dbFile string, idx *declcfg.PackageIndex) error {
	db, err := sqlite.Open(dbFile)
	if err != nil {
		return err
	}

	migrator, err := sqlite.NewSQLLiteMigrator(db)
	if err != nil {
		return err
	}
	if migrator == nil {
		return fmt.Errorf("failed to load migrator")
	}

	if err := migrator.Migrate(ctx); err != nil {
		return err
	}

	q := sqlite.NewSQLLiteQuerierFromDb(db)
	pkgNames, err := q.ListPackages(ctx)
	if err != nil {
		return err
	}
	for _, pkgName := range pkgNames {
		m, err := sqlite.PackageToModel(ctx, q, pkgName)
		if err != nil {
			return err
		}

		cfg := declcfg.ConvertFromModel(m)
		renderBundleObjects(&cfg)
		if err := idx.Add(&cfg); err != nil {
			return err
		}
	}
	return nil
}

func bundleToDeclcfg(bundle *registry.Bundle) (*declcfg.DeclarativeConfig, error) {
	bundleProperties, err := registry.PropertiesFromBundle(bundle)
	if err != nil {
		return nil, fmt.Errorf("get properties for bundle %q: %v", bundle.Name, err)
	}
	relatedImages, err := getRelatedImages(bundle)
	if err != nil {
		return nil, fmt.Errorf("get related images for bundle %q: %v", bundle.Name, err)
	}

	dBundle := declcfg.Bundle{
		Schema:        "olm.bundle",
		Name:          bundle.Name,
		Package:       bundle.Package,
		Image:         bundle.BundleImage,
		Properties:    bundleProperties,
		RelatedImages: relatedImages,
	}

	return &declcfg.DeclarativeConfig{Bundles: []declcfg.Bundle{dBundle}}, nil
}

func getRelatedImages(b *registry.Bundle) ([]declcfg.RelatedImage, error) {
	csv, err := b.ClusterServiceVersion()
	if err != nil {
		return nil, err
	}

	var objmap map[string]*json.RawMessage
	if err = json.Unmarshal(csv.Spec, &objmap); err != nil {
		return nil, err
	}

	rawValue, ok := objmap["relatedImages"]
	if !ok || rawValue == nil {
		return nil, err
	}

	var relatedImages []declcfg.RelatedImage
	if err = json.Unmarshal(*rawValue, &relatedImages); err != nil {
		return nil, err
	}
	return relatedImages, nil
}

func renderBundleObjects(cfg *declcfg.DeclarativeConfig) {
	for bi, b := range cfg.Bundles {
		props := b.Properties[:0]
		for _, p := range b.Properties {
			if p.Type != property.TypeBundleObject {
				props = append(props, p)
			}
		}

		for _, obj := range b.Objects {
			props = append(props, property.MustBuildBundleObjectData([]byte(obj)))
		}
		cfg.Bundles[bi].Properties = props
	}
}

func combineConfigs(cfgs []declcfg.DeclarativeConfig) *declcfg.DeclarativeConfig {
	out := &declcfg.DeclarativeConfig{}
	for _, in := range cfgs {
		out.Packages = append(out.Packages, in.Packages...)
		out.Bundles = append(out.Bundles, in.Bundles...)
		out.Others = append(out.Others, in.Others...)
	}
	return out
}
