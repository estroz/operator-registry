package declcfg

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/operator-framework/operator-registry/internal/model"
)

func TestDiffChannelsFrom(t *testing.T) {
	type spec struct {
		name         string
		inputBundles []*model.Bundle
		start, end   *model.Bundle
		expDiff      []*model.Bundle
	}

	bundles := []*model.Bundle{
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
			name:         "Success/v0.0.1",
			inputBundles: bundles,
			start:        bundles[0],
			end:          bundles[len(bundles)-1],
			expDiff:      bundles[1:],
		},
		{
			name:         "Success/v0.0.2",
			inputBundles: bundles,
			start:        bundles[1],
			end:          bundles[len(bundles)-1],
			expDiff:      bundles[2:],
		},
		{
			name:         "Success/v0.1.0",
			inputBundles: bundles,
			start:        bundles[3],
			end:          bundles[len(bundles)-1],
			expDiff:      bundles[4:],
		},
		{
			name:         "Success/v0.3.1",
			inputBundles: bundles,
			start:        bundles[len(bundles)-1],
			end:          bundles[len(bundles)-1],
			expDiff:      nil,
		},
	}
	for _, s := range specs {
		t.Run(s.name, func(t *testing.T) {
			newCh := &model.Channel{Name: "foo", Bundles: make(map[string]*model.Bundle, len(s.inputBundles))}
			for _, b := range s.inputBundles {
				newCh.Bundles[b.Name] = b
			}
			output, err := diffChannelBetweenNodes(newCh, s.start, s.end)
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
