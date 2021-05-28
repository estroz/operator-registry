package model

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiffChannelsFrom(t *testing.T) {
	type spec struct {
		name       string
		newBundles []*Bundle
		oldBundles []*Bundle
		start      *Bundle
		expDiff    []*Bundle
	}

	bundles1 := []*Bundle{
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
			oldBundles: bundles1[:1],
			start:      newReplacingBundle("anakin.v0.0.1", ""),
			expDiff:    bundles1[1:],
		},
		{
			name:       "Valid/v0.0.2",
			newBundles: bundles1,
			oldBundles: bundles1[:2],
			start:      newReplacingBundle("anakin.v0.0.2", "anakin.v0.0.1"),
			expDiff:    bundles1[2:],
		},
		{
			name:       "Valid/v0.1.0",
			newBundles: bundles1,
			oldBundles: bundles1[:4],
			start:      newReplacingBundle("anakin.v0.1.0", "anakin.v0.0.3"),
			expDiff:    bundles1[4:],
		},
		{
			name:       "Valid/v0.3.1",
			newBundles: bundles1,
			oldBundles: bundles1,
			start:      newReplacingBundle("anakin.v0.3.1", "anakin.v0.3.0"),
			expDiff:    nil,
		},
	}
	for _, s := range specs {
		t.Run(s.name, func(t *testing.T) {
			newCh := &Channel{Bundles: make(map[string]*Bundle, len(s.newBundles))}
			for _, b := range s.newBundles {
				newCh.Bundles[b.Name] = b
			}
			oldCh := &Channel{Bundles: make(map[string]*Bundle, len(s.oldBundles))}
			for _, b := range s.oldBundles {
				oldCh.Bundles[b.Name] = b
			}
			output, err := diffChannelsFrom(newCh, oldCh, s.start)
			require.NoError(t, err)
			sort.Slice(output, func(i, j int) bool {
				return output[i].Name < output[j].Name
			})
			assert.ElementsMatch(t,
				collectBundleReplaces(output),
				collectBundleReplaces(s.expDiff))
		})
	}
}

func newReplacingBundle(name, replaces string, skips ...string) *Bundle {
	return &Bundle{Name: name, Replaces: replaces, Skips: skips}
}

type bundleReplaces struct {
	Name, Replaces string
}

func collectBundleReplaces(bundles []*Bundle) (brs []bundleReplaces) {
	for _, b := range bundles {
		brs = append(brs, bundleReplaces{Name: b.Name, Replaces: b.Replaces})
	}
	return
}

func TestDiffFromOldChannelHeads(t *testing.T) {
	oldPkg := &Package{Name: "old"}
	oldModel := Model{oldPkg.Name: oldPkg}
	oldCh := &Channel{Name: "alpha", Package: oldPkg}
	oldPkg.Channels = map[string]*Channel{oldCh.Name: oldCh}
	oldPkg.DefaultChannel = oldCh
	oldBundle := &Bundle{Name: "operator.v0.1.0", Package: oldPkg, Channel: oldCh}
	oldCh.Bundles = map[string]*Bundle{oldBundle.Name: oldBundle}

	oldPkgCp := copyPackageEmptyChannels(oldPkg)
	newModel := Model{oldPkgCp.Name: oldPkgCp}
	oldChCp := copyChannelEmptyBundles(oldCh, oldPkgCp)
	oldPkgCp.Channels = map[string]*Channel{oldChCp.Name: oldChCp}
	oldPkgCp.DefaultChannel = oldChCp
	newBundle := &Bundle{Name: "operator.v0.1.1", Package: oldPkgCp, Channel: oldChCp, Replaces: oldBundle.Name}
	oldBundleCp := copyBundle(oldBundle, oldChCp, oldPkgCp)
	oldChCp.Bundles = map[string]*Bundle{
		oldBundleCp.Name: oldBundleCp,
		newBundle.Name:   newBundle,
	}

	diff, err := DiffFromOldChannelHeads(oldModel, newModel)
	assert.NoError(t, err)
	assert.Contains(t, diff, oldPkg.Name)
	assert.Len(t, diff, 1)
	assert.Contains(t, diff[oldPkg.Name].Channels, oldCh.Name)
	assert.Len(t, diff[oldPkg.Name].Channels, 1)
	assert.Equal(t, diff[oldPkg.Name].DefaultChannel.Name, oldCh.Name)
	assert.Len(t, diff[oldPkg.Name].Channels[oldCh.Name].Bundles, 1)
	assert.Contains(t, diff[oldPkg.Name].Channels[oldCh.Name].Bundles, newBundle.Name)
}
