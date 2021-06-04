package action_test

import (
	"context"
	"embed"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/operator-framework/operator-registry/internal/action"
	"github.com/operator-framework/operator-registry/internal/declcfg"
	"github.com/operator-framework/operator-registry/internal/property"
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
				Deps:     true,
			},
			expectedCfg: loadStatic(t, expDeclCfg),
			assertion:   require.NoError,
		},
	}

	for _, s := range specs {
		t.Run(s.name, func(t *testing.T) {
			actualCfg, actualErr := s.diff.Run(context.Background())
			s.assertion(t, actualErr)
			for bi := range actualCfg.Bundles {
				actualCfg.Bundles[bi].CsvJSON = ""
				actualCfg.Bundles[bi].Objects = nil
				b := actualCfg.Bundles[bi]
				var properties []property.Property
				for pi := range b.Properties {
					if b.Properties[pi].Type != property.TypeBundleObject {
						properties = append(properties, b.Properties[pi])
					}
				}
				actualCfg.Bundles[bi].Properties = properties
			}
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

func loadStatic(t *testing.T, staticCfg string) *declcfg.DeclarativeConfig {
	tmp, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(tmp) })
	if err := os.WriteFile(filepath.Join(tmp, "index.yaml"), []byte(staticCfg), 0777); err != nil {
		t.Fatal(err)
	}

	cfg, err := declcfg.LoadFS(os.DirFS(tmp))
	if err != nil {
		t.Fatal(err)
	}

	return cfg
}

const expDeclCfg = `---
defaultChannel: stable
name: bar
schema: olm.package
---
image: test.registry/bar-operator/bar-bundle:v1.0.0
name: bar.v1.0.0
package: bar
properties:
- type: olm.channel
  value:
    name: alpha
    replaces: bar.v0.2.0
- type: olm.channel
  value:
    name: stable
- type: olm.gvk
  value:
    group: test.bar
    kind: Bar
    version: v1
- type: olm.gvk
  value:
    group: test.bar
    kind: Bar
    version: v1alpha1
- type: olm.package
  value:
    packageName: bar
    version: 1.0.0
relatedImages:
- image: test.registry/bar-operator/bar:v1.0.0
  name: operator
schema: olm.bundle
---
defaultChannel: stable
name: baz
schema: olm.package
---
image: test.registry/baz-operator/baz-bundle:v1.1.0
name: baz.v1.1.0
package: baz
properties:
- type: olm.channel
  value:
    name: stable
    replaces: baz.v1.0.0
- type: olm.gvk
  value:
    group: test.baz
    kind: Baz
    version: v1
- type: olm.package
  value:
    packageName: baz
    version: 1.1.0
- type: olm.skips
  value: baz.v1.0.1
relatedImages:
- image: test.registry/baz-operator/baz:v1.1.0
  name: operator
schema: olm.bundle
---
defaultChannel: beta
name: foo
schema: olm.package
---
image: test.registry/foo-operator/foo-bundle:v0.3.1
name: foo.v0.3.1
package: foo
properties:
- type: olm.channel
  value:
    name: beta
    replaces: foo.v0.2.0
- type: olm.gvk
  value:
    group: test.foo
    kind: Foo
    version: v1
- type: olm.gvk
  value:
    group: test.foo
    kind: Foo
    version: v2
- type: olm.gvk.required
  value:
    group: test.bar
    kind: Bar
    version: v1alpha1
- type: olm.package
  value:
    packageName: foo
    version: 0.3.1
- type: olm.package.required
  value:
    packageName: bar
    versionRange: <0.2.0
- type: olm.skips
  value: foo.v0.3.0
relatedImages:
- image: test.registry/foo-operator/foo:v0.3.1
  name: operator
schema: olm.bundle
`
