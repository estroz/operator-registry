package model

import (
	"github.com/operator-framework/operator-registry/internal/property"

	"github.com/blang/semver"
)

// TODO: handle default channel updates, since newModel might have new default channel.
func DiffFromOldChannelHeads(oldModel, newModel Model) (Model, error) {
	diff := Model{}
	for _, newPkg := range newModel {
		diffPkg := copyPackageEmptyChannels(newPkg)
		diffPkg.Channels = make(map[string]*Channel)
		diff[diffPkg.Name] = diffPkg
		oldPkg, hasPkg := oldModel[newPkg.Name]
		if !hasPkg {
			oldPkg = copyPackageEmptyChannels(newPkg)
		}
		for _, newCh := range newPkg.Channels {
			diffCh := copyChannelEmptyBundles(newCh, diffPkg)
			diffPkg.Channels[diffCh.Name] = diffCh
			oldCh, hasCh := oldPkg.Channels[newCh.Name]
			if !hasCh {
				head, err := newCh.Head()
				if err != nil {
					return nil, err
				}
				diffHead := copyBundle(head, diffCh, diffPkg)
				diffCh.Bundles[diffHead.Name] = diffHead
			} else {
				oldHead, err := oldCh.Head()
				if err != nil {
					return nil, err
				}
				bundleDiff := getChannelReplacesGraph(newCh, oldHead) // Assumes oldHead exists in newCh.
				for _, b := range bundleDiff {
					diff.AddBundle(*copyBundle(b, diffCh, diffPkg))
				}
			}
		}

		diffPkg.DefaultChannel = diffPkg.Channels[newPkg.DefaultChannel.Name]
	}

	return diff, nil
}

func copyPackageEmptyChannels(in *Package) *Package {
	cp := &Package{
		Name:        in.Name,
		Description: in.Description,
		Icon: &Icon{
			Data:      make([]byte, len(in.Icon.Data)),
			MediaType: in.Icon.MediaType,
		},
		Channels: map[string]*Channel{},
	}
	copy(cp.Icon.Data, in.Icon.Data)
	return cp
}

func copyChannelEmptyBundles(in *Channel, pkg *Package) *Channel {
	cp := &Channel{
		Name:    in.Name,
		Package: pkg,
		Bundles: map[string]*Bundle{},
	}
	return cp
}

func copyBundle(in *Bundle, ch *Channel, pkg *Package) *Bundle {
	cp := &Bundle{
		Name:          in.Name,
		Channel:       ch,
		Package:       pkg,
		Image:         in.Image,
		Replaces:      in.Replaces, // TODO: null out?
		Skips:         make([]string, len(in.Skips)),
		Properties:    make([]property.Property, len(in.Properties)),
		RelatedImages: make([]RelatedImage, len(in.RelatedImages)),
		Version:       semver.MustParse(in.Version.String()),
	}
	copy(cp.Skips, in.Skips)
	copy(cp.Properties, in.Properties)
	cp.PropertiesP, _ = property.Parse(in.Properties)
	copy(cp.RelatedImages, in.RelatedImages)
	return cp
}

func getChannelReplacesGraph(ch *Channel, start *Bundle) (replacingBundles []*Bundle) {

	allReplaces := map[string][]*Bundle{}
	replacingStart := []*Bundle{}
	for _, b := range ch.Bundles {
		if b.Replaces == "" {
			continue
		}
		allReplaces[b.Replaces] = append(allReplaces[b.Replaces], b)
		if b.Replaces == start.Name {
			replacingStart = append(replacingStart, b)
		}
	}

	replacesSet := map[string]*Bundle{}
	for _, b := range replacingStart {
		currName := ""
		for next := []*Bundle{b}; len(next) > 0; next = next[1:] {
			currName = next[0].Name
			if _, seen := replacesSet[currName]; !seen {
				replacers := allReplaces[currName]
				next = append(next, replacers...)
				replacesSet[currName] = ch.Bundles[currName]
			}
		}
	}

	for _, b := range replacesSet {
		replacingBundles = append(replacingBundles, b)
	}

	return replacingBundles
}
