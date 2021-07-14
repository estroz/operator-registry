package action_test

import (
	"context"
	"embed"
	"io/fs"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/operator-framework/operator-registry/internal/action"
	"github.com/operator-framework/operator-registry/internal/declcfg"
	"github.com/operator-framework/operator-registry/pkg/image"
	"github.com/operator-framework/operator-registry/pkg/lib/bundle"
)

func TestDiff(t *testing.T) {
	type spec struct {
		name        string
		diff        action.Diff
		expectedCfg *declcfg.DeclarativeConfig
		assertion   require.ErrorAssertionFunc
	}

	registry, err := newDiffRegistry()
	require.NoError(t, err)

	specs := []spec{
		{
			name: "Success/Basic",
			diff: action.Diff{
				Registry: registry,
				OldRefs:  []string{filepath.Join("testdata", "indices", "old-declcfg")},
				NewRefs:  []string{filepath.Join("testdata", "indices", "latest-declcfg")},
			},
			expectedCfg: loadDirFS(t, indicesDir, filepath.Join("testdata", "indices", "exp-declcfg")),
			assertion:   require.NoError,
		},
	}

	for _, s := range specs {
		t.Run(s.name, func(t *testing.T) {
			actualCfg, actualErr := s.diff.Run(context.Background())
			s.assertion(t, actualErr)
			require.Equal(t, s.expectedCfg, actualCfg)
		})
	}
}

var (
	//go:embed testdata/foo-bundle-v0.1.0/manifests/*
	//go:embed testdata/foo-bundle-v0.1.0/metadata/*
	fooBundlev010 embed.FS
	//go:embed testdata/foo-bundle-v0.2.0/manifests/*
	//go:embed testdata/foo-bundle-v0.2.0/metadata/*
	fooBundlev020 embed.FS
	//go:embed testdata/foo-bundle-v0.3.0/manifests/*
	//go:embed testdata/foo-bundle-v0.3.0/metadata/*
	fooBundlev030 embed.FS
	//go:embed testdata/foo-bundle-v0.3.1/manifests/*
	//go:embed testdata/foo-bundle-v0.3.1/metadata/*
	fooBundlev031 embed.FS
	//go:embed testdata/bar-bundle-v0.1.0/manifests/*
	//go:embed testdata/bar-bundle-v0.1.0/metadata/*
	barBundlev010 embed.FS
	//go:embed testdata/bar-bundle-v0.2.0/manifests/*
	//go:embed testdata/bar-bundle-v0.2.0/metadata/*
	barBundlev020 embed.FS
	//go:embed testdata/bar-bundle-v1.0.0/manifests/*
	//go:embed testdata/bar-bundle-v1.0.0/metadata/*
	barBundlev100 embed.FS
	//go:embed testdata/baz-bundle-v1.0.0/manifests/*
	//go:embed testdata/baz-bundle-v1.0.0/metadata/*
	bazBundlev100 embed.FS
	//go:embed testdata/baz-bundle-v1.0.1/manifests/*
	//go:embed testdata/baz-bundle-v1.0.1/metadata/*
	bazBundlev101 embed.FS
	//go:embed testdata/baz-bundle-v1.1.0/manifests/*
	//go:embed testdata/baz-bundle-v1.1.0/metadata/*
	bazBundlev110 embed.FS
)

var bundleToFS = map[string]embed.FS{
	"test.registry/foo-operator/foo-bundle:v0.1.0": fooBundlev010,
	"test.registry/foo-operator/foo-bundle:v0.2.0": fooBundlev020,
	"test.registry/foo-operator/foo-bundle:v0.3.0": fooBundlev030,
	"test.registry/foo-operator/foo-bundle:v0.3.1": fooBundlev031,
	"test.registry/bar-operator/bar-bundle:v0.1.0": barBundlev010,
	"test.registry/bar-operator/bar-bundle:v0.2.0": barBundlev020,
	"test.registry/bar-operator/bar-bundle:v1.0.0": barBundlev100,
	"test.registry/baz-operator/baz-bundle:v1.0.0": bazBundlev100,
	"test.registry/baz-operator/baz-bundle:v1.0.1": bazBundlev101,
	"test.registry/baz-operator/baz-bundle:v1.1.0": bazBundlev110,
}

//go:embed testdata/indices
var indicesDir embed.FS

func newDiffRegistry() (image.Registry, error) {
	subDeclcfgImage, err := fs.Sub(indicesDir, "testdata/indices")
	if err != nil {
		return nil, err
	}
	const configsLabel = "operators.operatorframework.io.index.configs.v1"
	reg := &image.MockRegistry{
		RemoteImages: map[image.Reference]*image.MockImage{
			image.SimpleReference("test.registry/catalog/index-declcfg:latest"): {
				Labels: map[string]string{configsLabel: "/latest-declcfg/index.yaml"},
				FS:     subDeclcfgImage,
			},
			image.SimpleReference("test.registry/catalog/index-declcfg:old"): {
				Labels: map[string]string{configsLabel: "/old-declcfg/index.yaml"},
				FS:     subDeclcfgImage,
			},
		},
	}

	for name, bfs := range bundleToFS {
		base := filepath.Base(name)
		pkg := base[:strings.Index(base, ":")]
		base = strings.ReplaceAll(base, ":", "-")
		subImage, err := fs.Sub(bfs, path.Join("testdata", base))
		if err != nil {
			return nil, err
		}
		reg.RemoteImages[image.SimpleReference(name)] = &image.MockImage{
			Labels: map[string]string{bundle.PackageLabel: pkg},
			FS:     subImage,
		}
	}

	return reg, nil
}

func loadDirFS(t *testing.T, parent fs.FS, dir string) *declcfg.DeclarativeConfig {
	sub, err := fs.Sub(parent, dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := declcfg.LoadFS(sub)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}