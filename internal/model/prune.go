package model

import (
	"fmt"
	"sort"

	"github.com/blang/semver"

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

func PruneKeep(fromModel Model, pruneCfg pruneConfig, permissive, heads bool) (prunedModel Model, err error) {
	prunedModel = Model{}
	for _, pkg := range fromModel {
		prunePkg, keepPkg := pruneCfg[pkg.Name]
		if keepPkg || heads {
			cPkg := copyPackageEmptyChannels(pkg)
			prunedModel[pkg.Name] = cPkg
			for _, ch := range pkg.Channels {
				if keepPkg {
					pruneCh, keepChannel := prunePkg[ch.Name]
					if keepChannel || heads {
						cCh := copyChannelEmptyBundles(ch, cPkg)
						cPkg.Channels[cCh.Name] = cCh
						if keepChannel {
							for _, b := range ch.Bundles {
								if len(pruneCh) == 0 {
									prunedModel.AddBundle(*copyBundle(b, cCh, cPkg))
								} else {
									if _, keepBundle := pruneCh[b.Name]; keepBundle {
										prunedModel.AddBundle(*copyBundle(b, cCh, cPkg))
									}
								}
							}
						} else {
							head, err := ch.Head()
							if err != nil {
								return nil, err
							}
							prunedModel.AddBundle(*copyBundle(head, cCh, cPkg))
						}
					}
				} else {
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
	}

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

	for _, pkg := range fromModel {
		tmp := getProvidingBundles(pkg, reqGVKs, reqPkgs)
		for _, b := range tmp {
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

	return prunedModel, nil
}

func getProvidingBundles(pkg *Package, reqGVKs map[property.GVK]struct{}, reqPkgs map[string][]semver.Range) (providingBundles []*Bundle) {
	bundlesProvidingGVK := make(map[property.GVK][]*Bundle)
	var bundlesByRange [][]*Bundle
	ranges, isRequired := reqPkgs[pkg.Name]
	if isRequired {
		bundlesByRange = make([][]*Bundle, len(ranges))
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
	latestBundles := make(map[string]*Bundle)
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
	if len(missedRanges) == 0 {
		delete(reqPkgs, pkg.Name)
	}

	for _, b := range latestBundles {
		providingBundles = append(providingBundles, b)
	}

	return providingBundles
}

func getModelDependencies(m Model) (reqGVKs map[property.GVK]struct{}, reqPkgs map[string][]semver.Range, err error) {
	reqGVKs = map[property.GVK]struct{}{}
	reqPkgs = map[string][]semver.Range{}
	for _, pkg := range m {
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
							return nil, nil, err
						}
					} else {
						inRange = rangeAny
					}
					reqPkgs[pkgReq.PackageName] = append(reqPkgs[pkgReq.PackageName], inRange)
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
