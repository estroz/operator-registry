package model

import (
	"fmt"
	"sort"

	"github.com/blang/semver"
	"github.com/hashicorp/go-multierror"

	"github.com/operator-framework/operator-registry/internal/property"
)

type missingPruneKeyError struct {
	keyType string
	key     string
}

func (e missingPruneKeyError) Error() string {
	return fmt.Sprintf("%s prune key %q not found in config", e.keyType, e.key)
}

func PruneRemove(fromModel, pruneModel Model, permissive bool) (Model, error) {
	for _, ppkg := range pruneModel {
		pkg, hasPkg := fromModel[ppkg.Name]
		if !hasPkg {
			if !permissive {
				return nil, missingPruneKeyError{keyType: property.TypePackage, key: ppkg.Name}
			}
			continue
		}

		numChToRm := 0
		for _, pch := range ppkg.Channels {
			ch, hasChannel := pkg.Channels[pch.Name]
			if !hasChannel {
				if !permissive {
					return nil, missingPruneKeyError{keyType: property.TypeChannel, key: pch.Name}
				}
				continue
			}

			numBToRm := 0
			for _, pb := range pch.Bundles {
				b, hasBundle := ch.Bundles[pb.Name]
				if !hasBundle {
					if !permissive {
						return nil, missingPruneKeyError{keyType: "olm.bundle", key: pch.Name}
					}
					continue
				}

				b.TakeAction = true
				numBToRm++
			}
			if numBToRm == len(ch.Bundles) || (numBToRm == 0 && len(pch.Bundles) == 0) {
				ch.TakeAction = true
				numChToRm++
			}
		}
		pkg.TakeAction = numChToRm == len(pkg.Channels) || (numChToRm == 0 && len(ppkg.Channels) == 0)
	}

	if err := markDependencies(fromModel, false); err != nil {
		return nil, err
	}

	isRemove := func(action bool) bool { return action && true }
	return prune(fromModel, isRemove), nil
}

func PruneKeep(fromModel, pruneModel Model, permissive, heads bool) (Model, error) {
	for _, ppkg := range pruneModel {
		pkg, hasPkg := fromModel[ppkg.Name]
		if !hasPkg {
			if !permissive {
				return nil, missingPruneKeyError{keyType: property.TypePackage, key: ppkg.Name}
			}
			continue
		}

		// Channels
		numChToRm := 0
		if !heads {
			numChToRm = len(pkg.Channels)
		}
		for _, pch := range ppkg.Channels {
			ch, hasChannel := pkg.Channels[pch.Name]
			if !hasChannel {
				if !permissive {
					return nil, missingPruneKeyError{keyType: property.TypeChannel, key: pch.Name}
				}
				continue
			}

			// Bundles.
			numBToRm := len(ch.Bundles)
			if heads {
				head, err := ch.Head()
				if err != nil {
					return nil, err
				}
				rhead := ch.Bundles[head.Name]
				rhead.TakeAction = true
				numBToRm--
			}
			for _, pb := range pch.Bundles {
				b, hasBundle := ch.Bundles[pb.Name]
				if !hasBundle {
					if !permissive {
						return nil, missingPruneKeyError{keyType: "olm.bundle", key: pch.Name}
					}
					continue
				}

				b.TakeAction = true
				numBToRm--
			}
			if numBToRm < len(ch.Bundles) {
				ch.TakeAction = true
				numChToRm--
			}

		}
		pkg.TakeAction = numChToRm < len(pkg.Channels)
	}

	if err := markDependencies(fromModel, true); err != nil {
		return nil, err
	}

	isRemove := func(action bool) bool { return action && false }
	return prune(fromModel, isRemove), nil
}

func markDependencies(m Model, dependentAction bool) error {
	reqGVKs, reqPkgs, err := getRequiredDependencies(m, dependentAction)
	if err != nil {
		return err
	}
	for _, pkg := range m {
		bundlesProvidingGVK := make(map[property.GVK][]*Bundle)
		bundlesProvidingPkgRange := make([]*Bundle, 0)
		inRange, isOfPkg := reqPkgs[pkg.Name]
		for _, ch := range pkg.Channels {
			for _, b := range ch.Bundles {
				b := b
				for _, gvk := range b.PropertiesP.GVKs {
					if _, hasGVK := reqGVKs[gvk]; hasGVK {
						bundlesProvidingGVK[gvk] = append(bundlesProvidingGVK[gvk], b)
					}
				}
				if isOfPkg && inRange(b.Version) {
					bundlesProvidingPkgRange = append(bundlesProvidingPkgRange, b)
				}
			}
		}
		latestBundles := make(map[string]*Bundle)
		for gvk, bundles := range bundlesProvidingGVK {
			sort.Slice(bundles, func(i, j int) bool {
				return bundles[i].Version.LT(bundles[j].Version)
			})
			lb := bundles[len(bundles)-1]
			latestBundles[lb.Version.String()] = lb
			delete(reqGVKs, gvk)
		}
		if len(bundlesProvidingPkgRange) > 0 {
			sort.Slice(bundlesProvidingPkgRange, func(i, j int) bool {
				return bundlesProvidingPkgRange[i].Version.LT(bundlesProvidingPkgRange[j].Version)
			})
			lb := bundlesProvidingPkgRange[len(bundlesProvidingPkgRange)-1]
			latestBundles[lb.Version.String()] = lb
			delete(reqPkgs, pkg.Name)
		}

		if len(latestBundles) > 0 {
			pkg.TakeAction = dependentAction
		}
		for _, lb := range latestBundles {
			lb.TakeAction = dependentAction
			lb.Channel.TakeAction = dependentAction
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

	return result.ErrorOrNil()
}

func prune(m Model, isRemove func(bool) bool) Model {

	newModel := Model{}
	for _, pkg := range m {
		if isRemove(pkg.TakeAction) {
			continue
		}
		pkg.Channels = make(map[string]*Channel)
		newModel[pkg.Name] = pkg
		for _, ch := range pkg.Channels {
			if isRemove(ch.TakeAction) {
				continue
			}
			ch.Bundles = make(map[string]*Bundle)
			pkg.Channels[ch.Name] = ch
			for _, b := range ch.Bundles {
				if isRemove(b.TakeAction) {
					continue
				}
				newModel.AddBundle(*b)
			}
		}
	}

	// TODO: handle dangling replaces.

	return newModel
}

func rangeAny(semver.Version) bool { return true }

func getRequiredDependencies(m Model, dependentAction bool) (reqGVKs map[property.GVK]struct{}, reqPkgs map[string]semver.Range, err error) {
	reqGVKs = make(map[property.GVK]struct{})
	reqPkgs = make(map[string]semver.Range)
	for _, pkg := range m {
		if pkg.TakeAction && dependentAction {
			continue
		}
		for _, ch := range pkg.Channels {
			if ch.TakeAction && dependentAction {
				continue
			}
			for _, b := range ch.Bundles {
				if b.TakeAction && dependentAction {
					continue
				}
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
							return nil, nil, err
						}
					} else {
						inRange = rangeAny
					}
					if _, ok := reqPkgs[pkgReq.PackageName]; !ok {
						reqPkgs[pkgReq.PackageName] = inRange
					} else {
						reqPkgs[pkgReq.PackageName].OR(inRange)
					}
				}
			}
		}
	}

	return reqGVKs, reqPkgs, nil
}

func gvkSetToSlice(gvkSet map[property.GVK]struct{}) property.GVKs {
	gvks := make(property.GVKs, len(gvkSet))
	i := 0
	for gvk := range gvkSet {
		gvks[i] = gvk
		i++
	}
	sort.Sort(gvks)
	return gvks
}

func pkgSetToSlice(pkgSet map[string]semver.Range) []string {
	pkgs := make([]string, len(pkgSet))
	i := 0
	for pkg := range pkgSet {
		pkgs[i] = pkg
		i++
	}
	sort.Strings(pkgs)
	return pkgs
}
