package declcfg

import (
	"fmt"

	"github.com/blang/semver"
	"github.com/hashicorp/go-multierror"

	"github.com/operator-framework/operator-registry/internal/model"
	"github.com/operator-framework/operator-registry/internal/property"
)

func DiffFromOldChannelHeads(oldModel, newModel model.Model, withDeps bool) (diff model.Model, err error) {
	diff = model.Model{}
	for _, newPkg := range newModel {
		diffPkg := copyPackageEmptyChannels(newPkg)
		diffPkg.Channels = make(map[string]*model.Channel)
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
				diff.AddBundle(*copyBundle(head, diffCh, diffPkg))
			} else {
				oldHead, err := oldCh.Head()
				if err != nil {
					return nil, err
				}
				bundleDiff, err := diffChannelsFromNode(newCh, oldCh, oldHead)
				if err != nil {
					return nil, err
				}
				for _, b := range bundleDiff {
					diff.AddBundle(*copyBundle(b, diffCh, diffPkg))
				}
			}
		}

		diffPkg.DefaultChannel = diffPkg.Channels[newPkg.DefaultChannel.Name]
	}

	if !withDeps {
		return diff, nil
	}

	// Find all dependencies in the pruned model.
	reqGVKs := map[property.GVK]struct{}{}
	reqPkgs := map[string][]semver.Range{}
	for _, pkg := range diff {
		for _, ch := range pkg.Channels {
			for _, b := range ch.Bundles {
				for _, gvkReq := range b.PropertiesP.GVKsRequired {
					gvk := property.GVK{
						Group:   gvkReq.Group,
						Version: gvkReq.Version,
						Kind:    gvkReq.Kind,
					}
					reqGVKs[gvk] = struct{}{}
				}
				for _, pkgReq := range b.PropertiesP.PackagesRequired {
					var inRange semver.Range
					if pkgReq.VersionRange != "" {
						if inRange, err = semver.ParseRange(pkgReq.VersionRange); err != nil {
							// Should never happen since model has been validated.
							return nil, err
						}
					} else {
						inRange = rangeAny
					}
					reqPkgs[pkgReq.PackageName] = append(reqPkgs[pkgReq.PackageName], inRange)
				}
			}
		}
	}

	// Add dependencies from the full catalog.
	for _, pkg := range oldModel {
		// pkg, err := idx.LoadPackageModel(pkgName)
		// if err != nil {
		// 	return nil, err
		// }
		// TODO: might need to hydrate between these bundles so they can be upgraded between.
		for _, b := range getProvidingBundles(pkg, reqGVKs, reqPkgs) {
			ppkg, hasPkg := diff[b.Package.Name]
			if !hasPkg {
				ppkg = copyPackageEmptyChannels(b.Package)
				diff[ppkg.Name] = ppkg
			}
			pch, hasCh := ppkg.Channels[b.Channel.Name]
			if !hasCh {
				pch = copyChannelEmptyBundles(b.Channel, ppkg)
				ppkg.Channels[pch.Name] = pch
			}
			if _, hasBundle := pch.Bundles[b.Name]; !hasBundle {
				cb := copyBundle(b, pch, ppkg)
				diff.AddBundle(*cb)
			}
		}
	}

	// Remove unavailable replaces.
	for _, pkg := range diff {
		for _, ch := range pkg.Channels {
			for _, b := range ch.Bundles {
				if _, hasReplaces := ch.Bundles[b.Replaces]; !hasReplaces {
					b.Replaces = ""
				}
			}
		}
	}

	// Ensure both reqGVKs and reqPkgs are empty. It is likely a bug if they are not,
	// since the model is assumed to be valid.
	var result *multierror.Error
	if len(reqGVKs) != 0 {
		result = multierror.Append(result, fmt.Errorf("gvks not provided: %+q", gvkSetToSlice(reqGVKs)))
	}
	if len(reqPkgs) != 0 {
		result = multierror.Append(result, fmt.Errorf("packages not provided: %+q", pkgSetToSlice(reqPkgs)))
	}

	return diff, result.ErrorOrNil()
}

func copyPackageEmptyChannels(in *model.Package) *model.Package {
	cp := &model.Package{
		Name:        in.Name,
		Description: in.Description,
		Channels:    map[string]*model.Channel{},
	}
	if in.Icon != nil {
		cp.Icon = &model.Icon{
			Data:      make([]byte, len(in.Icon.Data)),
			MediaType: in.Icon.MediaType,
		}
		copy(cp.Icon.Data, in.Icon.Data)
	}
	return cp
}

func copyChannelEmptyBundles(in *model.Channel, pkg *model.Package) *model.Channel {
	cp := &model.Channel{
		Name:    in.Name,
		Package: pkg,
		Bundles: map[string]*model.Bundle{},
	}
	return cp
}

func copyBundle(in *model.Bundle, ch *model.Channel, pkg *model.Package) *model.Bundle {
	cp := &model.Bundle{
		Name:          in.Name,
		Channel:       ch,
		Package:       pkg,
		Image:         in.Image,
		Replaces:      in.Replaces,
		Skips:         make([]string, len(in.Skips)),
		Properties:    make([]property.Property, len(in.Properties)),
		RelatedImages: make([]model.RelatedImage, len(in.RelatedImages)),
		Version:       semver.MustParse(in.Version.String()),
	}
	copy(cp.Skips, in.Skips)
	copy(cp.Properties, in.Properties)
	cp.PropertiesP, _ = property.Parse(in.Properties)
	copy(cp.RelatedImages, in.RelatedImages)
	return cp
}

func diffChannelsFromNode(newCh, oldCh *model.Channel, start *model.Bundle) (replacingBundles []*model.Bundle, err error) {

	oldChain := map[string]*model.Bundle{start.Name: nil}
	for next := start; next != nil && next.Replaces != ""; next = oldCh.Bundles[next.Replaces] {
		oldChain[next.Replaces] = next
	}

	next, err := newCh.Head()
	if err != nil {
		return nil, err
	}
	var intersection string
	for next != nil && next.Replaces != "" {
		if _, inChain := oldChain[next.Replaces]; inChain {
			intersection = next.Replaces
			break
		}
		next = newCh.Bundles[next.Replaces]
	}

	if intersection == "" {
		bundles := map[string]*model.Bundle{}
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

	allReplaces := map[string][]*model.Bundle{}
	replacesIntersection := []*model.Bundle{}
	for _, b := range newCh.Bundles {
		if b.Replaces == "" {
			continue
		}
		allReplaces[b.Replaces] = append(allReplaces[b.Replaces], b)
		if b.Replaces == intersection {
			replacesIntersection = append(replacesIntersection, b)
		}
	}

	replacesSet := map[string]*model.Bundle{}
	for _, b := range replacesIntersection {
		currName := ""
		for next := []*model.Bundle{b}; len(next) > 0; next = next[1:] {
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
