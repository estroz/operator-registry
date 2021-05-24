package action

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/blang/semver"
	"github.com/sirupsen/logrus"

	"github.com/operator-framework/operator-registry/internal/declcfg"
	"github.com/operator-framework/operator-registry/internal/model"
	"github.com/operator-framework/operator-registry/internal/property"
)

type Pruner interface {
	Prune(declcfg.DeclarativeConfig) (declcfg.DeclarativeConfig, error)
}

type PruneConfig struct {
	*declcfg.DeclarativeConfig
}

func NewExclusivePruner(pc PruneConfig) Pruner {
	return &exclusivePruner{pc: pc}
}

func NewInclusivePruner(pc PruneConfig) Pruner {
	return &inclusivePruner{pc: pc}
}

func NewInclusiveHeadsPruner(pc PruneConfig) Pruner {
	return &inclusivePruner{pc: pc, heads: true}
}

type exclusivePruner struct {
	pc         PruneConfig
	permissive bool
}

func (p *exclusivePruner) Prune(from declcfg.DeclarativeConfig) (to declcfg.DeclarativeConfig, err error) {
	fromModel, err := declcfg.ConvertToModel(from)
	if err != nil {
		return to, err
	}
	fromRModel, err := toRModel(fromModel, false)
	if err != nil {
		return to, err
	}
	pruneModel, err := declcfg.ConvertToModel(*p.pc.DeclarativeConfig)
	if err != nil {
		return to, err
	}

	for _, pkg := range pruneModel {
		rpkg, hasRPkg := fromRModel[pkg.Name]
		if !hasRPkg && !p.permissive {
			return to, missingPruneKeyError{keyType: property.TypePackage, key: pkg.Name}
		} else if !hasRPkg {
			continue
		}

		numChToRm := 0
		for _, ch := range pkg.Channels {
			rch, hasRCh := rpkg.RChannels[ch.Name]
			if !hasRCh && !p.permissive {
				return to, missingPruneKeyError{keyType: property.TypeChannel, key: ch.Name}
			} else if !hasRCh {
				continue
			}

			numBToRm := 0
			for _, b := range ch.Bundles {
				rb, hasRB := rch.RBundles[b.Name]
				if !hasRB && !p.permissive {
					return to, missingPruneKeyError{keyType: "olm.bundle", key: ch.Name}
				} else if !hasRB {
					continue
				}

				rb.Remove = true
				numBToRm++
			}
			if numBToRm == len(rch.RBundles) || (numBToRm == 0 && len(ch.Bundles) == 0) {
				rch.Remove = true
				numChToRm++
			}
		}
		if numChToRm == len(rpkg.RChannels) || (numChToRm == 0 && len(pkg.Channels) == 0) {
			rpkg.Remove = true
		}
	}

	fromModel = prune(fromRModel)

	return declcfg.ConvertFromModel(fromModel), nil
}

type inclusivePruner struct {
	pc         PruneConfig
	heads      bool
	permissive bool
}

type missingPruneKeyError struct {
	keyType string
	key     string
}

func (e missingPruneKeyError) Error() string {
	return fmt.Sprintf("%s prune key %q not found in config", e.keyType, e.key)
}

func (p *inclusivePruner) Prune(from declcfg.DeclarativeConfig) (to declcfg.DeclarativeConfig, err error) {
	fromModel, err := declcfg.ConvertToModel(from)
	if err != nil {
		return to, err
	}
	fromRModel, err := toRModel(fromModel, true)
	if err != nil {
		return to, err
	}
	pruneModel, err := declcfg.ConvertToModel(*p.pc.DeclarativeConfig)
	if err != nil {
		return to, err
	}

	for _, pkg := range pruneModel {
		rpkg, hasRPkg := fromRModel[pkg.Name]
		if !hasRPkg && !p.permissive {
			return to, missingPruneKeyError{keyType: property.TypePackage, key: pkg.Name}
		} else if !hasRPkg {
			continue
		}

		// Channels
		numChToRm := 0
		if !p.heads {
			numChToRm = len(rpkg.RChannels)
		}
		for _, ch := range pkg.Channels {
			rch, hasRCh := rpkg.RChannels[ch.Name]
			if !hasRCh && !p.permissive {
				return to, missingPruneKeyError{keyType: property.TypeChannel, key: ch.Name}
			} else if !hasRCh {
				continue
			}

			// Bundles.
			numBToRm := len(rch.RBundles)
			if p.heads {
				head, err := rch.Head()
				if err != nil {
					return to, err
				}
				rhead := rch.RBundles[head.Name]
				rhead.Remove = false
				numBToRm--
			}
			for _, b := range ch.Bundles {
				rb, hasRB := rch.RBundles[b.Name]
				if !hasRB && !p.permissive {
					return to, missingPruneKeyError{keyType: "olm.bundle", key: ch.Name}
				} else if !hasRB {
					continue
				}

				rb.Remove = false
				numBToRm--
			}
			if numBToRm < len(rch.RBundles) {
				rch.Remove = false
				numChToRm--
			}

		}
		rpkg.Remove = numChToRm == len(rpkg.Channels)
	}

	fromModel = prune(fromRModel)

	return declcfg.ConvertFromModel(fromModel), nil
}

type RModel map[string]*RPackage

type RPackage struct {
	*model.Package
	RChannels map[string]*RChannel
	Remove    bool
}

type RChannel struct {
	*model.Channel
	RPackage *RPackage
	RBundles map[string]*RBundle
	Remove   bool
}

type RBundle struct {
	*model.Bundle
	RPackage   *RPackage
	RChannel   *RChannel
	Version    semver.Version
	Properties *property.Properties
	Remove     bool
}

func toRModel(from model.Model, initialRemove bool) (RModel, error) {
	rmodel := RModel{}
	for _, pkg := range from {
		pkg := pkg
		rpkg := &RPackage{}
		rpkg.Remove = initialRemove
		rpkg.Package = pkg
		rpkg.RChannels = make(map[string]*RChannel, len(pkg.Channels))
		rmodel[pkg.Name] = rpkg
		for _, ch := range pkg.Channels {
			ch := ch
			rch := &RChannel{}
			rch.Channel = ch
			rch.RPackage = rpkg
			rch.Remove = initialRemove
			rch.RBundles = make(map[string]*RBundle, len(ch.Bundles))
			rpkg.RChannels[ch.Name] = rch
			for _, b := range ch.Bundles {
				b := b
				rb := &RBundle{}
				rb.Bundle = b
				rb.RChannel = rch
				rb.RPackage = rpkg
				rb.Remove = initialRemove
				var err error
				if rb.Properties, err = property.Parse(b.Properties); err != nil {
					return nil, err
				}
				if rb.Version, err = getCSVVersion([]byte(b.CsvJSON)); err != nil {
					return nil, err
				}
				rch.RBundles[b.Name] = rb
			}
		}
	}

	return rmodel, nil
}

func prune(m RModel) model.Model {
	reqGVKs, reqPkgs := getRequiredDependencies(m)
	for _, pkg := range m {
		bundleGVKSet := make(map[property.GVK][]*RBundle)
		bundlesInPkgRange := make([]*RBundle, 0)
		inRange, isOfPkg := reqPkgs[pkg.Name]
		for _, ch := range pkg.RChannels {
			for _, b := range ch.RBundles {
				b := b
				for _, gvk := range b.Properties.GVKs {
					if _, hasGVK := reqGVKs[gvk]; hasGVK {
						bundleGVKSet[gvk] = append(bundleGVKSet[gvk], b)
					}
				}
				if isOfPkg && inRange(b.Version) {
					bundlesInPkgRange = append(bundlesInPkgRange, b)
				}
			}
		}
		latestBundles := make(map[string]*RBundle)
		for gvk, bundles := range bundleGVKSet {
			sort.Slice(bundles, func(i, j int) bool {
				return bundles[i].Version.LT(bundles[j].Version)
			})
			lb := bundles[len(bundles)-1]
			latestBundles[lb.Version.String()] = lb
			delete(reqGVKs, gvk)
		}
		sort.Slice(bundlesInPkgRange, func(i, j int) bool {
			return bundlesInPkgRange[i].Version.LT(bundlesInPkgRange[j].Version)
		})
		if len(bundlesInPkgRange) > 0 {
			lb := bundlesInPkgRange[len(bundlesInPkgRange)-1]
			latestBundles[lb.Version.String()] = lb
			delete(reqPkgs, pkg.Name)
		}

		if len(latestBundles) > 0 {
			pkg.Remove = false
		}
		for _, lb := range latestBundles {
			lb.Remove = false
			lb.RChannel.Remove = false
		}
	}

	// TODO: ensure both reqGVKs and reqPkgs are empty.

	newModel := model.Model{}
	for _, rpkg := range m {
		if rpkg.Remove {
			continue
		}
		pkg := rpkg.Package
		pkg.Channels = make(map[string]*model.Channel)
		newModel[pkg.Name] = pkg
		for _, rch := range rpkg.RChannels {
			if rch.Remove {
				continue
			}
			ch := rch.Channel
			ch.Bundles = make(map[string]*model.Bundle)
			pkg.Channels[ch.Name] = ch
			for _, rb := range rch.RBundles {
				if rb.Remove {
					continue
				}
				newModel.AddBundle(*rb.Bundle)
			}
		}
	}

	// TODO: handle dangling replaces.

	return newModel
}

func rangeAny(semver.Version) bool { return true }

func getRequiredDependencies(m RModel) (reqGVKs map[property.GVK]struct{}, reqPkgs map[string]semver.Range) {
	reqGVKs = make(map[property.GVK]struct{})
	reqPkgs = make(map[string]semver.Range)
	for _, pkg := range m {
		if pkg.Remove {
			continue
		}
		for _, ch := range pkg.RChannels {
			if ch.Remove {
				continue
			}
			for _, b := range ch.RBundles {
				if b.Remove {
					continue
				}
				for _, gvkReq := range b.Properties.GVKsRequired {
					gvk := property.GVK{
						Group:   gvkReq.Group,
						Version: gvkReq.Version,
						Kind:    gvkReq.Kind,
					}
					reqGVKs[gvk] = struct{}{}
				}
				for _, pkgReq := range b.Properties.PackagesRequired {
					var inRange semver.Range
					if pkgReq.VersionRange != "" {
						var err error
						if inRange, err = semver.ParseRange(pkgReq.VersionRange); err != nil {
							// Should never happen since model has been validated.
							logrus.Error(err)
							continue
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

	return reqGVKs, reqPkgs
}

func getCSVVersion(csvJSON []byte) (semver.Version, error) {
	var tmp struct {
		Spec struct {
			Version semver.Version `json:"version"`
		} `json:"spec"`
	}
	err := json.Unmarshal(csvJSON, &tmp)
	return tmp.Spec.Version, err
}
