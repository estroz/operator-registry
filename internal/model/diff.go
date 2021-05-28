package model

import (
	"fmt"

	"github.com/operator-framework/operator-registry/internal/property"

	"github.com/blang/semver"
)

// TODO: add new GVK/package dependencies.
func DiffFromOldChannelHeads(oldModel, newModel Model) (Model, error) {
	diff := Model{}
	for _, newPkg := range newModel {
		fmt.Println("package", newPkg.Name)
		diffPkg := copyPackageEmptyChannels(newPkg)
		diffPkg.Channels = make(map[string]*Channel)
		diff[diffPkg.Name] = diffPkg
		oldPkg, hasPkg := oldModel[newPkg.Name]
		if !hasPkg {
			oldPkg = copyPackageEmptyChannels(newPkg)
		}
		for _, newCh := range newPkg.Channels {
			fmt.Println("\tchannel", newCh.Name)
			diffCh := copyChannelEmptyBundles(newCh, diffPkg)
			diffPkg.Channels[diffCh.Name] = diffCh
			oldCh, hasCh := oldPkg.Channels[newCh.Name]
			if !hasCh {
				head, err := newCh.Head()
				if err != nil {
					return nil, err
				}
				diffHead := copyBundle(head, diffCh, diffPkg)
				fmt.Println("\t\tadding diff head", diffHead.Name, "to", diffCh.Name)
				// Since this head is the only bundle in diffCh, it replaces nothing.
				diffHead.Replaces = ""
				diffCh.Bundles[diffHead.Name] = diffHead
			} else {
				oldHead, err := oldCh.Head()
				if err != nil {
					return nil, err
				}
				fmt.Println("\t\tfrom old head", oldHead.Name)
				bundleDiff, err := diffChannelsFrom(newCh, oldCh, oldHead)
				if err != nil {
					return nil, err
				}
				for _, b := range bundleDiff {
					fmt.Println("\t\t\tadding", b.Name, "to", b.Channel.Name)
					diff.AddBundle(*copyBundle(b, diffCh, diffPkg))
				}
			}
		}

		diffPkg.DefaultChannel = diffPkg.Channels[newPkg.DefaultChannel.Name]
		fmt.Println()
	}

	return diff, nil
}

func copyPackageEmptyChannels(in *Package) *Package {
	cp := &Package{
		Name:        in.Name,
		Description: in.Description,
		Channels:    map[string]*Channel{},
	}
	if in.Icon != nil {
		cp.Icon = &Icon{
			Data:      make([]byte, len(in.Icon.Data)),
			MediaType: in.Icon.MediaType,
		}
		copy(cp.Icon.Data, in.Icon.Data)
	}
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
		Replaces:      in.Replaces,
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

func diffChannelsFrom(newCh, oldCh *Channel, start *Bundle) (replacingBundles []*Bundle, err error) {

	oldChain := map[string]*Bundle{start.Name: nil}
	for next := start; next != nil && next.Replaces != ""; next = oldCh.Bundles[next.Replaces] {
		oldChain[next.Replaces] = next
	}

	next, err := newCh.Head()
	if err != nil {
		return nil, err
	}
	fmt.Println("new head:", next.Name, "replaces", next.Replaces)
	var intersection string
	for next != nil && next.Replaces != "" {
		if _, inChain := oldChain[next.Replaces]; inChain {
			intersection = next.Replaces
			fmt.Println("intersection:", intersection)
			break
		}
		next = newCh.Bundles[next.Replaces]
	}

	if intersection == "" {
		fmt.Println("no intersection")
		bundles := map[string]*Bundle{}
		for _, b := range oldCh.Bundles {
			bundles[b.Name] = b
		}
		for _, b := range newCh.Bundles {
			bundles[b.Name] = b
		}
		for _, b := range bundles {
			replacingBundles = append(replacingBundles, b)
		}
		return replacingBundles, nil
	}

	allReplaces := map[string][]*Bundle{}
	replacesIntersection := []*Bundle{}
	for _, b := range newCh.Bundles {
		if b.Replaces == "" {
			continue
		}
		allReplaces[b.Replaces] = append(allReplaces[b.Replaces], b)
		if b.Replaces == intersection {
			replacesIntersection = append(replacesIntersection, b)
		}
	}

	replacesSet := map[string]*Bundle{}
	for _, b := range replacesIntersection {
		currName := ""
		for next := []*Bundle{b}; len(next) > 0; next = next[1:] {
			currName = next[0].Name
			if _, seen := replacesSet[currName]; !seen {
				replacers := allReplaces[currName]
				next = append(next, replacers...)
				replacesSet[currName] = newCh.Bundles[currName]
			}
		}
	}

	for _, b := range replacesSet {
		if _, inOldCh := oldCh.Bundles[b.Name]; !inOldCh {
			replacingBundles = append(replacingBundles, b)
		}
	}

	return replacingBundles, nil
}
