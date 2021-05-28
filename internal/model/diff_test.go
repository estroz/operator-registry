package model

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetReplacesGraph(t *testing.T) {
	type spec struct {
		name    string
		bundles []*Bundle
		start   *Bundle
		chain   []*Bundle
	}

	bundles1 := []*Bundle{
		newReplacingBundle("anakin.v0.0.1", ""),
		newReplacingBundle("anakin.v0.0.2", "anakin.v0.0.1"),
		newReplacingBundle("anakin.v0.0.3", "anakin.v0.0.2"),
		newReplacingBundle("anakin.v0.1.0", "anakin.v0.0.3"),
		newReplacingBundle("anakin.v0.1.1", "anakin.v0.1.0"),
		newReplacingBundle("anakin.v0.2.0", "anakin.v0.1.0"),
		newReplacingBundle("anakin.v0.3.0", "anakin.v0.2.0"),
		newReplacingBundle("anakin.v0.3.1", "anakin.v0.3.0"),
	}

	specs := []spec{
		{
			name:    "Valid/v0.0.1",
			bundles: bundles1,
			start:   newReplacingBundle("anakin.v0.0.1", ""),
			chain:   bundles1[1:],
		},
		{
			name:    "Valid/v0.0.2",
			bundles: bundles1,
			start:   newReplacingBundle("anakin.v0.0.2", "anakin.v0.0.1"),
			chain:   bundles1[2:],
		},
		{
			name:    "Valid/v0.1.0",
			bundles: bundles1,
			start:   newReplacingBundle("anakin.v0.1.0", "anakin.v0.0.3"),
			chain:   bundles1[4:],
		},
		{
			name:    "Valid/v0.3.1",
			bundles: bundles1,
			start:   newReplacingBundle("anakin.v0.3.1", "anakin.v0.3.0"),
			chain:   nil,
		},
	}
	for _, s := range specs {
		t.Run(s.name, func(t *testing.T) {
			ch := &Channel{Bundles: make(map[string]*Bundle, len(s.bundles))}
			for _, b := range s.bundles {
				ch.Bundles[b.Name] = b
			}
			output := ch.GetReplacesGraph(s.start)
			sort.Slice(output, func(i, j int) bool {
				return output[i].Name < output[j].Name
			})
			assert.ElementsMatch(t,
				collectBundleReplaces(output),
				collectBundleReplaces(s.chain))
		})
	}
}

func newReplacingBundle(name, replaces string) *Bundle {
	return &Bundle{Name: name, Replaces: replaces}
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
