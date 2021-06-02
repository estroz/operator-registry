package declcfg

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"k8s.io/apimachinery/pkg/util/yaml"
)

// PackageIndex loads declarative config packages from a filesystem cache.
// Create one with IndexDir() if the set of packages is too big to load into
// memory with LoadDir().
type PackageIndex struct {
	fs          afero.Fs
	cacheDir    string
	pkgEncoders map[string]encoder
	cleanups    []func()
	cleanedUp   bool
}

// IndexDir caches all configs for each package in a package file
// in order to load a single package into memory later via the returned
// PackageIndex.
func IndexDir(configDir string) (idx *PackageIndex, err error) {
	idx = &PackageIndex{
		fs:          afero.NewOsFs(),
		pkgEncoders: map[string]encoder{},
	}
	if err := idx.indexDir(configDir); err != nil {
		return nil, err
	}
	return idx, nil
}

func (idx *PackageIndex) indexDir(configDir string) (err error) {
	defer func() {
		for _, cleanup := range idx.cleanups {
			cleanup()
		}
	}()
	if idx.cacheDir, err = afero.TempDir(idx.fs, "", "declcfg_cache."); err != nil {
		return fmt.Errorf("error creating cache dir: %v", err)
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
				if pkgName = in.Package; pkgName == "" {
					pkgName = globalName
				}
				pkgName += ".object"
				obj = in
			}

			enc, err := idx.getPackageEncoder(pkgName)
			if err != nil {
				return fmt.Errorf("error getting writer for package %q: %v", pkgName, err)
			}
			if err := enc.Encode(obj); err != nil {
				return fmt.Errorf("error encoding config object for package %q: %v", pkgName, err)
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

type indexCacheModifiedError struct {
	underlying error
}

func (e indexCacheModifiedError) Error() string {
	return fmt.Sprintf("%v; index cache files may have been modified", e.underlying)
}

func (e indexCacheModifiedError) Unwrap() error {
	return e.underlying
}

func newIndexCacheModErr(msgf string, vals ...interface{}) error {
	return indexCacheModifiedError{underlying: fmt.Errorf(msgf, vals...)}
}

// LoadPackage loads the package delcaration and all the package bundles and objects
// for pkgName from the PackageIndex's filesystem cache.
func (idx PackageIndex) LoadPackage(pkgName string) (*DeclarativeConfig, error) {
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
		return nil, newIndexCacheModErr("no package config for package %q found", pkgName)
	case 1:
		if cpkgName := cfg.Packages[0].Name; cpkgName != pkgName {
			return nil, newIndexCacheModErr("package %q found instead of %q", cpkgName, pkgName)
		}
	default:
		pkgNames := make([]string, pl)
		for i, pkg := range cfg.Packages {
			pkgNames[i] = pkg.Name
		}
		return nil, newIndexCacheModErr("multiple package configs for package %q found (%q)",
			pkgName, strings.Join(pkgNames, ","))
	}

	return cfg, nil
}

// Cleanup deletes the package index. Run this once done using a PackageIndex.
// A cleaned-up PackageIndex cannot be used again.
func (idx *PackageIndex) Cleanup() error {
	idx.cleanedUp = true
	return idx.fs.RemoveAll(idx.cacheDir)
}
