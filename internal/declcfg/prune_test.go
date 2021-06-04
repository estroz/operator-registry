package declcfg

import (
	"sort"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/operator-framework/operator-registry/internal/model"
)

func TestDiffChannelsFrom(t *testing.T) {
	type spec struct {
		name       string
		newBundles []*model.Bundle
		start, end *model.Bundle
		expDiff    []*model.Bundle
	}

	bundles1 := []*model.Bundle{
		newReplacingBundle("anakin.v0.0.1", ""),
		newReplacingBundle("anakin.v0.0.2", "anakin.v0.0.1"),
		newReplacingBundle("anakin.v0.0.3", "anakin.v0.0.2"),
		newReplacingBundle("anakin.v0.1.0", "anakin.v0.0.3"),
		newReplacingBundle("anakin.v0.1.1", "anakin.v0.1.0"),
		newReplacingBundle("anakin.v0.2.0", "anakin.v0.1.0", "anakin.v0.1.1"),
		newReplacingBundle("anakin.v0.3.0", "anakin.v0.2.0"),
		newReplacingBundle("anakin.v0.3.1", "anakin.v0.3.0"),
	}

	specs := []spec{
		{
			name:       "Valid/v0.0.1",
			newBundles: bundles1,
			start:      bundles1[0],
			end:        bundles1[len(bundles1)-1],
			expDiff:    bundles1[1:],
		},
		{
			name:       "Valid/v0.0.2",
			newBundles: bundles1,
			start:      bundles1[1],
			end:        bundles1[len(bundles1)-1],
			expDiff:    bundles1[2:],
		},
		{
			name:       "Valid/v0.1.0",
			newBundles: bundles1,
			start:      bundles1[3],
			end:        bundles1[len(bundles1)-1],
			expDiff:    bundles1[4:],
		},
		{
			name:       "Valid/v0.3.1",
			newBundles: bundles1,
			start:      bundles1[len(bundles1)-1],
			end:        bundles1[len(bundles1)-1],
			expDiff:    nil,
		},
	}
	for _, s := range specs {
		t.Run(s.name, func(t *testing.T) {
			newCh := &model.Channel{Bundles: make(map[string]*model.Bundle, len(s.newBundles))}
			allReplaces := map[string][]*model.Bundle{}
			for _, b := range s.newBundles {
				b := b
				newCh.Bundles[b.Name] = b
				if b.Replaces != "" {
					allReplaces[b.Replaces] = append(allReplaces[b.Replaces], b)
				}
			}
			output, err := diffChannelBetweenNodes(newCh, s.start, s.end, allReplaces)
			assert.NoError(t, err)
			sort.Slice(output, func(i, j int) bool {
				return output[i].Name < output[j].Name
			})
			assert.ElementsMatch(t,
				collectBundleReplaces(output),
				collectBundleReplaces(s.expDiff))
		})
	}
}

func newReplacingBundle(name, replaces string, skips ...string) *model.Bundle {
	return &model.Bundle{Name: name, Replaces: replaces, Skips: skips}
}

type bundleReplaces struct {
	Name, Replaces string
}

func collectBundleReplaces(bundles []*model.Bundle) (brs []bundleReplaces) {
	for _, b := range bundles {
		brs = append(brs, bundleReplaces{Name: b.Name, Replaces: b.Replaces})
	}
	return
}

func TestPruneRemove(t *testing.T) {
	oldPkg := &model.Package{Name: "old"}
	oldCh := &model.Channel{Name: "alpha", Package: oldPkg}
	oldPkg.Channels = map[string]*model.Channel{oldCh.Name: oldCh}
	oldPkg.DefaultChannel = oldCh
	oldBundle := &model.Bundle{Name: "operator.v0.1.0", Package: oldPkg, Channel: oldCh}
	oldCh.Bundles = map[string]*model.Bundle{oldBundle.Name: oldBundle}

	oldPkgCp := copyPackageEmptyChannels(oldPkg)
	newModel := model.Model{oldPkgCp.Name: oldPkgCp}
	oldChCp := copyChannelEmptyBundles(oldCh, oldPkgCp)
	oldPkgCp.Channels = map[string]*model.Channel{oldChCp.Name: oldChCp}
	oldPkgCp.DefaultChannel = oldChCp
	newBundle := &model.Bundle{Name: "operator.v0.1.1", Package: oldPkgCp, Channel: oldChCp, Replaces: oldBundle.Name}
	oldBundleCp := copyBundle(oldBundle, oldChCp, oldPkgCp)
	oldChCp.Bundles = map[string]*model.Bundle{
		oldBundleCp.Name: oldBundleCp,
		newBundle.Name:   newBundle,
	}

	pruneConfig := PruneConfig{
		Packages: []Pkg{
			{Name: "old", Channels: []Channel{
				{Name: "alpha", Head: "operator.v0.1.0"}},
			},
		},
	}

	diff, err := PruneRemove(indexerFor(t, newModel), pruneConfig, false, false, false)
	require.NoError(t, err)
	require.NotNil(t, diff)
	require.Contains(t, diff, oldPkg.Name)
	require.Len(t, diff, 1)
	require.Contains(t, diff, oldPkg.Name)
	require.Len(t, diff[oldPkg.Name].Channels, 1)
	require.Contains(t, diff[oldPkg.Name].Channels, oldCh.Name)
	require.Equal(t, diff[oldPkg.Name].DefaultChannel.Name, oldCh.Name)
	require.Len(t, diff[oldPkg.Name].Channels[oldCh.Name].Bundles, 1)
	require.Contains(t, diff[oldPkg.Name].Channels[oldCh.Name].Bundles, newBundle.Name)
}

func indexerFor(t *testing.T, ms ...model.Model) *PackageIndex {
	idx := &PackageIndex{
		fs:          afero.NewMemMapFs(),
		pkgEncoders: map[string]encoder{},
	}
	for _, m := range ms {
		cfg := ConvertFromModel(m)
		require.NoError(t, idx.Add(&cfg))
	}
	return idx
}
