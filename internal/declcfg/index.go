package declcfg

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/operator-framework/operator-registry/internal/model"
)

// PackageIndex loads declarative config packages from a filesystem cache.
// Create one with IndexDir() if the set of packages is too big to load into
// memory with LoadDir().
type PackageIndex struct {
	fs          afero.Fs
	cacheDir    string
	cacheOnce   sync.Once
	pkgEncoders map[string]encoder
	cleanups    []func()
	cleanedUp   bool
}

func NewPackageIndex() *PackageIndex {
	return &PackageIndex{
		fs:          afero.NewOsFs(),
		pkgEncoders: map[string]encoder{},
	}
}

// IndexDir caches all configs for each package in a package file
// in order to load a single package into memory later via the returned
// PackageIndex.
func IndexDir(configDir string) (*PackageIndex, error) {
	idx := NewPackageIndex()
	if err := idx.IndexDir(configDir); err != nil {
		return nil, err
	}
	return idx, nil
}

func (idx *PackageIndex) init() (err error) {
	idx.cacheOnce.Do(func() {
		if idx.cacheDir, err = afero.TempDir(idx.fs, "", "declcfg_cache."); err != nil {
			err = fmt.Errorf("error creating cache dir: %v", err)
			return
		}
	})
	return err
}

// Add adds cfg to a PackageIndex. Add can be called multiple times on different cfgs.
func (idx *PackageIndex) Add(cfg *DeclarativeConfig) error {
	if err := idx.init(); err != nil {
		return err
	}

	for _, pkg := range cfg.Packages {
		if err := idx.add(pkg.Name, pkg); err != nil {
			return err
		}
	}
	for _, b := range cfg.Bundles {
		if err := idx.add(b.Package, b); err != nil {
			return err
		}
	}
	for _, other := range cfg.Others {
		if err := idx.add(defaultObjectName(other), other); err != nil {
			return err
		}
	}

	return nil
}

func defaultObjectName(meta Meta) (pkgName string) {
	if pkgName = meta.Package; pkgName == "" {
		pkgName = globalName
	}
	return pkgName + ".object"
}

func (idx *PackageIndex) add(pkgName string, obj interface{}) error {
	enc, err := idx.getPackageEncoder(pkgName)
	if err != nil {
		return fmt.Errorf("error getting writer for package %q: %v", pkgName, err)
	}
	if err := enc.Encode(obj); err != nil {
		return fmt.Errorf("error encoding config object for package %q: %v", pkgName, err)
	}
	return nil
}

// IndexDir caches all configs for each package in a package file
// in order to load a single package into memory later via PackageIndex.
// IndexDir can be called multiple times on different configDirs.
func (idx *PackageIndex) IndexDir(configDir string) (err error) {
	if err := idx.init(); err != nil {
		return err
	}

	walker := &dirWalker{fs: idx.fs}
	if err := walker.WalkFiles(configDir, func(path string, r io.Reader) error {
		dec := yaml.NewYAMLOrJSONDecoder(r, 4096)
		for {
			doc := json.RawMessage{}
			if err := dec.Decode(&doc); err != nil {
				break
			}
			doc = []byte(htmlReplacer.Replace(string(doc)))

			var in Meta
			if err := json.Unmarshal(doc, &in); err != nil {
				// Ignore JSON blobs if they are not parsable as meta objects.
				continue
			}

			var pkgName string
			var obj interface{}
			switch in.Schema {
			case schemaPackage:
				var p Package
				if err := json.Unmarshal(doc, &p); err != nil {
					return fmt.Errorf("parse package: %v", err)
				}
				pkgName = p.Name
				obj = p
			case schemaBundle:
				var b Bundle
				if err := json.Unmarshal(doc, &b); err != nil {
					return fmt.Errorf("parse bundle: %v", err)
				}
				pkgName = b.Package
				obj = b
			case "":
				// Ignore meta blobs that don't have a schema.
				continue
			default:
				// TODO: enable loading these.
				pkgName = defaultObjectName(in)
				obj = in
			}

			if err := idx.add(pkgName, obj); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to read declarative configs dir: %v", err)
	}

	return nil
}

// getPackageEncoder returns an encoder for pkgName, to which all configs for that package
// will be encoded. If one does not exist for pkgName, one will be created backed by a file
// in its own cache directory.
func (idx *PackageIndex) getPackageEncoder(pkgName string) (_ encoder, err error) {
	enc, ok := idx.pkgEncoders[pkgName]
	if !ok {
		pkgDir := filepath.Join(idx.cacheDir, pkgName)
		if err := idx.fs.MkdirAll(pkgDir, os.ModeDir|os.ModePerm); err != nil {
			return nil, err
		}
		// TODO: consider gob encoding for a speedup.
		indexPath := filepath.Join(pkgDir, "index.json")
		f, err := idx.fs.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, os.ModePerm)
		if err != nil {
			return nil, err
		}
		idx.cleanups = append(idx.cleanups, func() {
			if cerr := f.Close(); cerr != nil {
				logrus.Error(cerr)
			}
		})
		e := json.NewEncoder(f)
		e.SetEscapeHTML(false)
		idx.pkgEncoders[pkgName] = e
		enc = e
	}
	return enc, nil
}

// GetPackageNames returns all package names for typed declarative documents.
func (idx *PackageIndex) GetPackageNames() (pkgNames []string) {
	for pkgName := range idx.pkgEncoders {
		if !strings.HasSuffix(pkgName, ".object") {
			pkgNames = append(pkgNames, pkgName)
		}
	}
	return pkgNames
}

type indexCacheModifiedError struct {
	underlying error
}

func (e indexCacheModifiedError) Error() string {
	return fmt.Sprintf("%v; index cache files may have been modified", e.underlying)
}

func (e indexCacheModifiedError) Unwrap() error {
	return e.underlying
}

func newIndexCacheModError(msgf string, vals ...interface{}) error {
	return indexCacheModifiedError{underlying: fmt.Errorf(msgf, vals...)}
}

// LoadPackageModel calls LoadPackageConfig(pkgName) then calls ConvertToModel() on the result.
func (idx *PackageIndex) LoadPackageModel(pkgName string) (*model.Package, error) {
	cfg, err := idx.LoadPackageConfig(pkgName)
	if err != nil {
		return nil, err
	}
	m, err := ConvertToModel(*cfg)
	if err != nil {
		return nil, err
	}
	return m[pkgName], nil
}

// LoadPackageConfig loads the package delcaration and all the package bundles and objects
// for pkgName from the PackageIndex's filesystem cache.
func (idx *PackageIndex) LoadPackageConfig(pkgName string) (*DeclarativeConfig, error) {
	if idx.fs == nil {
		return nil, errors.New("package indexer is not initialized")
	}
	if idx.cleanedUp {
		return nil, errors.New("package indexer is already cleaned up")
	}

	walker := &dirWalker{fs: idx.fs}
	cfg, err := loadFS(filepath.Join(idx.cacheDir, pkgName), walker)
	if err != nil {
		return nil, err
	}

	switch pl := len(cfg.Packages); pl {
	case 0:
		if len(cfg.Bundles) == 0 {
			return nil, newIndexCacheModError("no package config for package %q found", pkgName)
		}
		for _, b := range cfg.Bundles {
			if b.Package != pkgName {
				return nil, newIndexCacheModError("package %q found instead of %q", b.Package, pkgName)
			}
		}
	case 1:
		if cpkgName := cfg.Packages[0].Name; cpkgName != pkgName {
			return nil, newIndexCacheModError("package %q found instead of %q", cpkgName, pkgName)
		}
	default:
		pkgNames := make([]string, pl)
		for i, pkg := range cfg.Packages {
			pkgNames[i] = pkg.Name
		}
		return nil, newIndexCacheModError("multiple package configs for package %q found (%q)",
			pkgName, strings.Join(pkgNames, ","))
	}

	return cfg, nil
}

// Cleanup deletes the package index. Run this once done using a PackageIndex.
// A cleaned-up PackageIndex cannot be used again.
func (idx *PackageIndex) Cleanup() error {
	for _, cleanup := range idx.cleanups {
		cleanup()
	}
	if idx.cacheDir != "" {
		if err := idx.fs.RemoveAll(idx.cacheDir); err != nil {
			return err
		}
	}
	idx.cleanedUp = true
	return nil
}
