package declcfg

import (
	"fmt"
	"sort"

	"github.com/blang/semver"
	"github.com/sirupsen/logrus"

	"github.com/operator-framework/operator-registry/internal/model"
	"github.com/operator-framework/operator-registry/internal/property"
)

type DiffOptions struct {
	Deps bool
}

func DiffFromHeads(oldModel, newModel model.Model, opts DiffOptions) (model.Model, error) {
	outputModel := model.Model{}

	if err := fillFromOldHeads(oldModel, newModel, outputModel); err != nil {
		return nil, err
	}

	// Add all channel heads not added above from the new to the output model.
	for _, newPkg := range newModel {
		if _, oldHasPkg := oldModel[newPkg.Name]; oldHasPkg {
			continue
		}
		// This package is newly added, otherwise some old channel existed
		// and was included in the above fillFromOldHeads call.
		outputPkg := copyPackageEmptyChannels(newPkg)
		outputModel[outputPkg.Name] = outputPkg
		for _, newCh := range newPkg.Channels {
			if _, oldHasCh := oldModel[newCh.Name]; oldHasCh {
				continue
			}
			// This channel is newly added, otherwise some old head existed
			// and was included in the above fillFromOldHeads call.
			outputCh := copyChannelEmptyBundles(newCh, outputPkg)
			outputPkg.Channels[outputCh.Name] = outputCh
			newHead, err := newCh.Head()
			if err != nil {
				return nil, err
			}
			outputModel.AddBundle(*copyBundle(newHead, outputCh, outputPkg))
		}
	}

	if opts.Deps {
		if err := addDependencies(oldModel, newModel, outputModel); err != nil {
			return nil, err
		}
	}

	return outputModel, nil
}

func fillFromOldHeads(oldModel, newModel, outputModel model.Model) error {
	for _, oldPkg := range oldModel {
		newPkg, newHasPkg := newModel[oldPkg.Name]
		if !newHasPkg {
			logrus.Debugf("old package %q not found in new model", oldPkg.Name)
			continue
		}
		outputPkg, outputHasPkg := outputModel[oldPkg.Name]
		if !outputHasPkg {
			outputPkg = copyPackageEmptyChannels(newPkg)
			outputModel[oldPkg.Name] = outputPkg
		}

		for _, oldCh := range oldPkg.Channels {
			newCh, newHasCh := newPkg.Channels[oldCh.Name]
			if !newHasCh {
				logrus.Debugf("old package %q channel %q not found in new package", oldPkg.Name, oldCh.Name)
				continue
			}
			newHead, err := newCh.Head()
			if err != nil {
				return err
			}
			oldHead, err := oldCh.Head()
			if err != nil {
				return err
			}
			if _, newHasOldHead := newCh.Bundles[oldHead.Name]; !newHasOldHead {
				logrus.Debugf("old package %q channel %q head %q not found in new channel", oldPkg.Name, oldCh.Name, oldHead.Name)
				continue
			}
			// No new bundles have been published in this channel, so add nothing.
			if oldHead.Name == newHead.Name {
				continue
			}

			// fmt.Printf("old head %s, new head %s\n", oldHead.Name, newHead.Name)
			bundleDiff, err := diffChannelBetweenNodes(newCh, oldHead, newHead)
			if err != nil {
				return err
			}
			outputCh, outputHasCh := outputPkg.Channels[newCh.Name]
			if !outputHasCh {
				outputCh = copyChannelEmptyBundles(newCh, outputPkg)
				outputPkg.Channels[newCh.Name] = outputCh
			}
			for _, newBundle := range bundleDiff {
				outputModel.AddBundle(*copyBundle(newBundle, outputCh, outputPkg))
			}
		}

		// Set default channel since it may have changed.
		if outputDefaultChannel, outputHasDefault := outputPkg.Channels[newPkg.DefaultChannel.Name]; outputHasDefault {
			outputPkg.DefaultChannel = outputDefaultChannel
		} else {
			outputPkg.DefaultChannel = copyChannelEmptyBundles(newPkg.DefaultChannel, outputPkg)
		}
	}

	return nil
}

func diffChannelBetweenNodes(ch *model.Channel, start, end *model.Bundle) (replacingBundles []*model.Bundle, err error) {
	// Construct the old replaces chain from start.
	oldChain := map[string]*model.Bundle{start.Name: nil}
	for next := start; next != nil && next.Replaces != ""; next = ch.Bundles[next.Replaces] {
		oldChain[next.Replaces] = next
		fmt.Printf("OLD CHAIN: %s -> %s\n", next.Name, next.Replaces)
	}

	// Trace the new replaces chain from end until the old chain intersects.
	var intersection string
	for next := end; next != nil && next.Replaces != ""; next = ch.Bundles[next.Replaces] {
		fmt.Printf("NEW CHAIN: %s -> %s\n", next.Name, next.Replaces)
		if _, inChain := oldChain[next.Replaces]; inChain {
			intersection = next.Replaces
			break
		}
	}

	// TODO: handle this better somehow, since start and end are not in
	// the same replaces chain and therefore cannot be upgraded between.
	if intersection == "" {
		for _, b := range ch.Bundles {
			replacingBundles = append(replacingBundles, b)
		}
		return replacingBundles, nil
	}

	// Find all bundles that replace the intersection via BFS,
	// i.e. the set of bundles that fill the update graph between start and end.
	allReplaces := map[string][]*model.Bundle{}
	for _, b := range ch.Bundles {
		if b.Replaces == "" {
			continue
		}
		allReplaces[b.Replaces] = append(allReplaces[b.Replaces], b)
	}
	replacesIntersection := allReplaces[intersection]
	replacesSet := map[string]*model.Bundle{}
	for _, b := range replacesIntersection {
		currName := ""
		for next := []*model.Bundle{b}; len(next) > 0; next = next[1:] {
			currName = next[0].Name
			if _, seen := replacesSet[currName]; !seen {
				replacers := allReplaces[currName]
				next = append(next, replacers...)
				replacesSet[currName] = ch.Bundles[currName]
			}
		}
	}

	// Remove every bundle between start and intersection inclusively,
	// which must already exist in the destination catalog
	for next := start; next != nil && next.Name != intersection; next = ch.Bundles[next.Replaces] {
		delete(replacesSet, next.Name)
	}

	for _, b := range replacesSet {
		replacingBundles = append(replacingBundles, b)
	}
	return replacingBundles, nil
}

func rangeAny(semver.Version) bool { return true }

func addDependencies(oldModel, newModel, outputModel model.Model) (err error) {
	// Find all dependencies of bundles in the output model.
	reqGVKs := map[property.GVK]struct{}{}
	reqPkgs := map[string][]semver.Range{}
	for _, pkg := range outputModel {
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
							return err
						}
					} else {
						inRange = rangeAny
					}
					reqPkgs[pkgReq.PackageName] = append(reqPkgs[pkgReq.PackageName], inRange)
				}
			}
		}
	}

	// Add dependencies from the new model if they are not present in the old model.
	for _, newPkg := range newModel {
		for _, b := range getProvidingBundles(newPkg, reqGVKs, reqPkgs) {
			outputPkg, outputHasPkg := outputModel[b.Package.Name]
			if !outputHasPkg {
				outputPkg = copyPackageEmptyChannels(b.Package)
				outputModel[outputPkg.Name] = outputPkg
			}
			outputCh, outputHasCh := outputPkg.Channels[b.Channel.Name]
			if !outputHasCh {
				outputCh = copyChannelEmptyBundles(b.Channel, outputPkg)
				outputPkg.Channels[outputCh.Name] = outputCh
			}
			if _, outputHasBundle := outputCh.Bundles[b.Name]; !outputHasBundle {
				if oldPkg, oldHasPkg := oldModel[b.Package.Name]; oldHasPkg {
					if oldCh, oldHasCh := oldPkg.Channels[b.Channel.Name]; oldHasCh {
						if _, oldHasBundle := oldCh.Bundles[b.Name]; oldHasBundle {
							continue
						}
					}
				}
				outputBundle := copyBundle(b, outputCh, outputPkg)
				outputModel.AddBundle(*outputBundle)
			}
		}
	}

	return nil
}

func getProvidingBundles(pkg *model.Package, reqGVKs map[property.GVK]struct{}, reqPkgs map[string][]semver.Range) (providingBundles []*model.Bundle) {
	bundlesProvidingGVK := make(map[property.GVK][]*model.Bundle)
	var bundlesByRange [][]*model.Bundle
	ranges, isPkgRequired := reqPkgs[pkg.Name]
	if isPkgRequired {
		bundlesByRange = make([][]*model.Bundle, len(ranges))
	}
	for _, ch := range pkg.Channels {
		for _, b := range ch.Bundles {
			b := b
			for _, gvk := range b.PropertiesP.GVKs {
				if _, hasGVK := reqGVKs[gvk]; hasGVK {
					bundlesProvidingGVK[gvk] = append(bundlesProvidingGVK[gvk], b)
				}
			}
			for i, inRange := range ranges {
				if inRange(b.Version) {
					bundlesByRange[i] = append(bundlesByRange[i], b)
				}
			}
		}
	}
	latestBundles := make(map[string]*model.Bundle)
	for gvk, bundles := range bundlesProvidingGVK {
		sort.Slice(bundles, func(i, j int) bool {
			return bundles[i].Version.LT(bundles[j].Version)
		})
		lb := bundles[len(bundles)-1]
		latestBundles[lb.Version.String()] = lb
		delete(reqGVKs, gvk)
	}
	missedRanges := []semver.Range{}
	for i, bundlesInRange := range bundlesByRange {
		if len(bundlesInRange) == 0 {
			missedRanges = append(missedRanges, ranges[i])
			continue
		}
		sort.Slice(bundlesInRange, func(i, j int) bool {
			return bundlesInRange[i].Version.LT(bundlesInRange[j].Version)
		})
		lb := bundlesInRange[len(bundlesInRange)-1]
		latestBundles[lb.Version.String()] = lb
	}
	if isPkgRequired && len(missedRanges) == 0 {
		delete(reqPkgs, pkg.Name)
	}

	for _, b := range latestBundles {
		providingBundles = append(providingBundles, b)
	}

	return providingBundles
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
