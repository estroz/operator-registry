package declcfg

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexDir(t *testing.T) {
	expectFoo := fooPkg + fooBundle010 + fooBundle020
	expectBar := barPkg + barBundle010 + barBundle020

	type spec struct {
		name       string
		indexes    []string
		assertion  require.ErrorAssertionFunc
		expectPkgs map[string]string
	}
	specs := []spec{
		{
			name:       "Success/SinglePackageFile",
			indexes:    []string{fooPkg + fooBundle010 + fooBundle020},
			assertion:  require.NoError,
			expectPkgs: map[string]string{"foo": expectFoo},
		},
		{
			name:       "Success/SingleFile",
			indexes:    []string{fooPkg + fooBundle010 + fooBundle020 + barPkg + barBundle010 + barBundle020},
			assertion:  require.NoError,
			expectPkgs: map[string]string{"foo": expectFoo, "bar": expectBar},
		},
		{
			name:       "Success/TwoFile",
			indexes:    []string{fooPkg + fooBundle010 + fooBundle020, barPkg + barBundle010 + barBundle020},
			assertion:  require.NoError,
			expectPkgs: map[string]string{"foo": expectFoo, "bar": expectBar},
		},
		{
			name:       "Success/MultipleFile",
			indexes:    []string{fooPkg, fooBundle010, fooBundle020, barPkg, barBundle010, barBundle020},
			assertion:  require.NoError,
			expectPkgs: map[string]string{"foo": expectFoo, "bar": expectBar},
		},
	}

	const configsDir = "/configs"
	for _, s := range specs {
		t.Run(s.name, func(t *testing.T) {
			idx := &PackageIndex{
				fs:          afero.NewMemMapFs(),
				pkgEncoders: map[string]encoder{},
			}
			subFS := afero.NewBasePathFs(idx.fs, configsDir)
			for i, indexData := range s.indexes {
				err := afero.WriteFile(subFS, fmt.Sprintf("%d.yaml", i), []byte(indexData), os.ModePerm)
				require.NoError(t, err)
			}
			s.assertion(t, idx.IndexDir(configsDir))

			infos, err := afero.ReadDir(idx.fs, idx.cacheDir)
			require.NoError(t, err)
			assert.Len(t, infos, len(s.expectPkgs))
			for _, dirInfo := range infos {
				// Wrote the expected directory.
				assert.True(t, dirInfo.IsDir())
				assert.Contains(t, s.expectPkgs, dirInfo.Name())

				// Loads from a file successfully.
				cfg, err := idx.LoadPackageConfig(dirInfo.Name())
				require.NoError(t, err)

				// Equal output.
				expectPkg := s.expectPkgs[dirInfo.Name()]
				buf := bytes.Buffer{}
				require.NoError(t, WriteYAML(*cfg, &buf))
				assert.Equal(t, expectPkg, buf.String())
			}

			require.NoError(t, idx.Cleanup())
			_, err = idx.LoadPackageConfig("somepkg")
			require.Error(t, err)
			require.Equal(t, "package indexer is already cleaned up", err.Error())
		})
	}
}

const fooPkg = `---
defaultChannel: beta
name: foo
schema: olm.package
`

const fooBundle010 = `---
image: test.registry/foo-operator/foo-bundle:v0.1.0
name: foo.v0.1.0
package: foo
properties:
- type: olm.channel
  value:
    name: beta
- type: olm.package
  value:
    packageName: foo
    version: 0.1.0
schema: olm.bundle
`

const fooBundle020 = `---
image: test.registry/foo-operator/foo-bundle:v0.2.0
name: foo.v0.2.0
package: foo
properties:
- type: olm.channel
  value:
    name: beta
- type: olm.package
  value:
    packageName: foo
    version: 0.2.0
schema: olm.bundle
`

const barPkg = `---
defaultChannel: alpha
name: bar
schema: olm.package
`

const barBundle010 = `---
image: test.registry/bar-operator/bar-bundle:v0.1.0
name: bar.v0.1.0
package: bar
properties:
- type: olm.channel
  value:
    name: alpha
- type: olm.package
  value:
    packageName: bar
    version: 0.1.0
schema: olm.bundle
`

const barBundle020 = `---
image: test.registry/bar-operator/bar-bundle:v0.2.0
name: bar.v0.2.0
package: bar
properties:
- type: olm.channel
  value:
    name: alpha
- type: olm.package
  value:
    packageName: bar
    version: 0.2.0
schema: olm.bundle
`
