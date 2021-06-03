package declcfg

import (
	"fmt"
	"sort"

	"github.com/blang/semver"
	"github.com/hashicorp/go-multierror"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/operator-framework/operator-registry/internal/model"
	"github.com/operator-framework/operator-registry/internal/property"
)

type missingPruneKeyError struct {
	keyType string
	key     string
}

func (e missingPruneKeyError) Error() string {
	return fmt.Sprintf("%s prune key %q not found in config", e.keyType, e.key)
}

type pruneConfig map[string]map[string]map[string]struct{}

func rangeAny(semver.Version) bool { return true }

func PruneKeep(idx *PackageIndex, pruneCfg pruneConfig, permissive, heads bool) (prunedModel model.Model, err error) {
	pkgNames := idx.GetPackageNames()
	pkgNameSet := sets.NewString(pkgNames...)
	prunedModel = model.Model{}
	if heads {
		for _, pkgName := range pkgNames {
			pkg, err := idx.LoadPackageModel(pkgName)
			if err != nil {
				return nil, err
			}
			cPkg := copyPackageEmptyChannels(pkg)
			prunedModel[cPkg.Name] = cPkg
			for _, ch := range pkg.Channels {
				cCh := copyChannelEmptyBundles(ch, cPkg)
				cPkg.Channels[cCh.Name] = cCh
				head, err := ch.Head()
				if err != nil {
					return nil, err
				}
				prunedModel.AddBundle(*copyBundle(head, cCh, cPkg))
			}
		}
	}
	for pkgName, pruneChannels := range pruneCfg {
		if !pkgNameSet.Has(pkgName) {
			if !permissive {
				return nil, missingPruneKeyError{keyType: property.TypePackage, key: pkgName}
			}
			continue
		}
		pkg, err := idx.LoadPackageModel(pkgName)
		if err != nil {
			return nil, err
		}
		if !heads {
			prunedModel[pkgName] = copyPackageEmptyChannels(pkg)
		}
		cPkg := prunedModel[pkgName]
		if len(pruneChannels) == 0 {
			for _, ch := range pkg.Channels {
				cPkg.Channels[ch.Name] = ch
			}
		}
		for chName, pruneBundles := range pruneChannels {
			ch, hasCh := pkg.Channels[chName]
			if !hasCh {
				if !permissive {
					return nil, missingPruneKeyError{keyType: property.TypeChannel, key: chName}
				}
				continue
			}
			if !heads {
				cPkg.Channels[chName] = copyChannelEmptyBundles(ch, cPkg)
			}
			cCh := cPkg.Channels[chName]
			cPkg.Channels[cCh.Name] = cCh
			if len(pruneBundles) == 0 {
				for _, b := range ch.Bundles {
					prunedModel.AddBundle(*b)
				}
			}
			for bName := range pruneBundles {
				b, hasBundle := ch.Bundles[bName]
				if !hasBundle {
					if !permissive {
						return nil, missingPruneKeyError{keyType: "olm.bundle", key: bName}
					}
					continue
				}
				if heads {
					if _, created := cCh.Bundles[bName]; created {
						continue
					}
				}
				prunedModel.AddBundle(*copyBundle(b, cCh, cPkg))
			}
		}
	}

	// TODO: clear replaces on truncated channels.

	reqGVKs := map[property.GVK]struct{}{}
	reqPkgs := map[string][]semver.Range{}
	for _, pkg := range prunedModel {
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

	for _, pkgName := range pkgNames {
		pkg, err := idx.LoadPackageModel(pkgName)
		if err != nil {
			return nil, err
		}
		for _, b := range getProvidingBundles(pkg, reqGVKs, reqPkgs) {
			ppkg, hasPkg := prunedModel[b.Package.Name]
			if !hasPkg {
				ppkg = copyPackageEmptyChannels(b.Package)
				prunedModel[ppkg.Name] = ppkg
			}
			pch, hasCh := ppkg.Channels[b.Channel.Name]
			if !hasCh {
				pch = copyChannelEmptyBundles(b.Channel, ppkg)
				ppkg.Channels[pch.Name] = pch
			}
			cb := copyBundle(b, pch, ppkg)
			prunedModel.AddBundle(*cb)
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

	return prunedModel, result.ErrorOrNil()
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

func pkgSetToSlice(pkgSet map[string][]semver.Range) []string {
	pkgs := make([]string, len(pkgSet))
	i := 0
	for pkg := range pkgSet {
		pkgs[i] = pkg
		i++
	}
	sort.Strings(pkgs)
	return pkgs
}
