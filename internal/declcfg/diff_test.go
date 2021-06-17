package declcfg

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

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
