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

type missingDiffKeyError struct {
	keyType string
	key     string
}

func (e missingDiffKeyError) Error() string {
	return fmt.Sprintf("%s diff key %q not found in config", e.keyType, e.key)
}

type DiffConfig struct {
	Packages []DiffPackage `json:"packages"`
}

type DiffPackage struct {
	Name     string        `json:"name"`
	Channels []DiffChannel `json:"channels"`
	Bundles  []string      `json:"bundles"`
}

type DiffChannel struct {
	Name    string   `json:"name"`
	Head    string   `json:"head"`
	Bundles []string `json:"bundles"`
}

func ConvertToDiffConfig(dcfg *DeclarativeConfig) (pcfg DiffConfig, err error) {
	m, err := ConvertToModel(*dcfg)
	if err != nil {
		return pcfg, err
	}
	for _, pkg := range m {
		ppkg := DiffPackage{Name: pkg.Name}
		for _, ch := range pkg.Channels {
			head, err := ch.Head()
			if err != nil {
				return pcfg, err
			}
			pch := DiffChannel{Name: ch.Name, Head: head.Name}
			for bName := range ch.Bundles {
				pch.Bundles = append(pch.Bundles, bName)
			}
			ppkg.Channels = append(ppkg.Channels, pch)
		}
		pcfg.Packages = append(pcfg.Packages, ppkg)
	}
	return pcfg, nil
}

type DiffOptions struct {
	Heads      bool
	Deps       bool
	Permissive bool
	Fill       bool
}

func DiffIndex(idx *PackageIndex, diffCfg DiffConfig, opts DiffOptions) (diffdModel model.Model, err error) {
	if opts.Fill {
		diffdModel, err = diffFill(idx, diffCfg, opts.Permissive, opts.Heads, opts.Deps)
	} else {
		diffdModel, err = diffExact(idx, diffCfg, opts.Permissive, opts.Heads, opts.Deps)
	}

	return diffdModel, err
}

func diffFill(idx *PackageIndex, diffCfg DiffConfig, permissive, heads, deps bool) (diffdModel model.Model, err error) {
	diffSet := make(map[string]map[string]map[string]struct{}, len(diffCfg.Packages))
	for _, pkg := range diffCfg.Packages {
		diffSet[pkg.Name] = make(map[string]map[string]struct{}, len(pkg.Channels))
		for _, ch := range pkg.Channels {
			diffSet[pkg.Name][ch.Name] = make(map[string]struct{}, len(ch.Bundles))
			for _, b := range ch.Bundles {
				diffSet[pkg.Name][ch.Name][b] = struct{}{}
			}
		}
	}

	pkgNames := idx.GetPackageNames()
	pkgNameSet := sets.NewString(pkgNames...)
	diffdModel = model.Model{}

	// Add all channel heads from the full catalog to the model.
	if heads {
		for _, pkgName := range pkgNames {
			pkg, err := idx.LoadPackageModel(pkgName)
			if err != nil {
				return nil, err
			}
			cPkg := copyPackageEmptyChannels(pkg)
			diffdModel[cPkg.Name] = cPkg
			for _, ch := range pkg.Channels {
				cCh := copyChannelEmptyBundles(ch, cPkg)
				cPkg.Channels[cCh.Name] = cCh
				head, err := ch.Head()
				if err != nil {
					return nil, err
				}
				diffdModel.AddBundle(*copyBundle(head, cCh, cPkg))
			}
		}
	}

	// Add all packages, channels, and bundles (package versions) in diffSet
	// from the full catalog to the model.
	for pkgName, diffChannels := range diffSet {
		if !pkgNameSet.Has(pkgName) {
			if !permissive {
				return nil, missingDiffKeyError{keyType: property.TypePackage, key: pkgName}
			}
			continue
		}
		pkg, err := idx.LoadPackageModel(pkgName)
		if err != nil {
			return nil, err
		}
		if !heads {
			diffdModel[pkgName] = copyPackageEmptyChannels(pkg)
		}
		cPkg := diffdModel[pkgName]
		if len(diffChannels) == 0 {
			for _, ch := range pkg.Channels {
				cPkg.Channels[ch.Name] = ch
			}
		}
		for chName, diffBundles := range diffChannels {
			ch, hasCh := pkg.Channels[chName]
			if !hasCh {
				if !permissive {
					return nil, missingDiffKeyError{keyType: property.TypeChannel, key: chName}
				}
				continue
			}
			if !heads {
				cPkg.Channels[chName] = copyChannelEmptyBundles(ch, cPkg)
			}
			cCh := cPkg.Channels[chName]
			cPkg.Channels[cCh.Name] = cCh
			if len(diffBundles) == 0 {
				for _, b := range ch.Bundles {
					diffdModel.AddBundle(*b)
				}
			}
			for bName := range diffBundles {
				b, hasBundle := ch.Bundles[bName]
				if !hasBundle {
					if !permissive {
						return nil, missingDiffKeyError{keyType: "olm.bundle", key: bName}
					}
					continue
				}
				if heads {
					if _, created := cCh.Bundles[bName]; created {
						continue
					}
				}
				diffdModel.AddBundle(*copyBundle(b, cCh, cPkg))
			}
		}
	}

	if deps {
		if err := addDependencies(idx, diffdModel); err != nil {
			return nil, err
		}
	}

	fixReplaces(diffdModel)

	return diffdModel, nil
}

func diffExact(idx *PackageIndex, diffCfg DiffConfig, permissive, heads, deps bool) (diff model.Model, err error) {
	diffSet := make(map[string]map[string]map[string]int, len(diffCfg.Packages))
	pkgLookup := make(map[string]DiffPackage, len(diffCfg.Packages))
	for _, pkg := range diffCfg.Packages {
		diffSet[pkg.Name] = make(map[string]map[string]int, len(pkg.Channels))
		pkgLookup[pkg.Name] = pkg
		for ci, ch := range pkg.Channels {
			diffSet[pkg.Name][ch.Name] = make(map[string]int, len(ch.Bundles))
			for _, b := range ch.Bundles {
				diffSet[pkg.Name][ch.Name][b] = ci
			}
		}
	}

	pkgNames := idx.GetPackageNames()
	diff = model.Model{}

	for _, pkgName := range pkgNames {
		newPkg, err := idx.LoadPackageModel(pkgName)
		if err != nil {
			return nil, err
		}
		diffCfgChannels, hasPkg := diffSet[newPkg.Name]
		if hasPkg || heads {
			diffPkg := copyPackageEmptyChannels(newPkg)
			diff[diffPkg.Name] = diffPkg
		}
		if !hasPkg || len(diffCfgChannels) == 0 {
			if heads {
				diffPkg := diff[newPkg.Name]
				for _, newCh := range newPkg.Channels {
					diffCh := copyChannelEmptyBundles(newCh, diffPkg)
					diffPkg.Channels[diffCh.Name] = diffCh
					head, err := newCh.Head()
					if err != nil {
						return nil, err
					}
					diff.AddBundle(*copyBundle(head, diffCh, diffPkg))
				}
			}
			continue
		}
		diffPkg := diff[newPkg.Name]
		for chName, diffCfgBundles := range diffCfgChannels {
			newCh, hasCh := newPkg.Channels[chName]
			if hasCh || heads {
				diffCh := copyChannelEmptyBundles(newCh, diffPkg)
				diffPkg.Channels[diffCh.Name] = diffCh
			}
			if !hasCh || len(diffCfgBundles) == 0 {
				if heads {
					diffCh := diffPkg.Channels[newCh.Name]
					head, err := newCh.Head()
					if err != nil {
						return nil, err
					}
					diff.AddBundle(*copyBundle(head, diffCh, diffPkg))
				}
				continue
			}
			allReplaces := map[string][]*model.Bundle{}
			for _, b := range newCh.Bundles {
				if b.Replaces == "" {
					continue
				}
				allReplaces[b.Replaces] = append(allReplaces[b.Replaces], b)
			}
			diffCh := diffPkg.Channels[newCh.Name]
			for bName, chIdx := range diffCfgBundles {
				if pkg, hasLPkg := pkgLookup[diffPkg.Name]; hasLPkg {
					if ch := pkg.Channels[chIdx]; ch.Head == "" {
						newBundle := newCh.Bundles[bName]
						diff.AddBundle(*copyBundle(newBundle, diffCh, diffPkg))
					} else {
						oldHead, hasHead := newCh.Bundles[ch.Head]
						if !hasHead {
							// TODO: request full package to find intersection.
							continue
						}
						newHead, err := newCh.Head()
						if err != nil {
							return nil, err
						}
						bundleDiff, err := diffChannelBetweenNodes(newCh, oldHead, newHead, allReplaces)
						if err != nil {
							return nil, err
						}
						for _, newBundle := range bundleDiff {
							diff.AddBundle(*copyBundle(newBundle, diffCh, diffPkg))
						}
					}
				}
			}
		}

		diffPkg.DefaultChannel = diffPkg.Channels[newPkg.DefaultChannel.Name]
	}

	if deps {
		if err := addDependencies(idx, diff); err != nil {
			return nil, err
		}
	}

	fixReplaces(diff)

	return diff, nil
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

func diffChannelBetweenNodes(ch *model.Channel, start, end *model.Bundle, allReplaces map[string][]*model.Bundle) (replacingBundles []*model.Bundle, err error) {
	// There is no diff if start is end.
	if start.Name == end.Name {
		return nil, nil
	}

	// Construct the old replaces chain from start.
	oldChain := map[string]*model.Bundle{start.Name: nil}
	for next := start; next != nil && next.Replaces != ""; next = ch.Bundles[next.Replaces] {
		oldChain[next.Replaces] = next
	}

	// Trace the new replaces chain from end until the old chain intersects.
	var intersection string
	for next := end; next != nil && next.Replaces != ""; next = ch.Bundles[next.Replaces] {
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

	// Remove every bundle between start and intersection,
	// which must already exist in the destination catalog
	for next := start; next != nil && next.Replaces != intersection; next = ch.Bundles[next.Replaces] {
		delete(replacesSet, next.Name)
	}

	for _, b := range replacesSet {
		replacingBundles = append(replacingBundles, b)
	}
	return replacingBundles, nil
}

func rangeAny(semver.Version) bool { return true }

func addDependencies(idx *PackageIndex, m model.Model) (err error) {
	// Find all dependencies in the diffd model.
	reqGVKs := map[property.GVK]struct{}{}
	reqPkgs := map[string][]semver.Range{}
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

	// Add dependencies from the full catalog.
	for _, pkgName := range idx.GetPackageNames() {
		pkg, err := idx.LoadPackageModel(pkgName)
		if err != nil {
			return err
		}
		// TODO: might need to hydrate between these bundles so they can be upgraded between.
		for _, b := range getProvidingBundles(pkg, reqGVKs, reqPkgs) {
			ppkg, hasPkg := m[b.Package.Name]
			if !hasPkg {
				ppkg = copyPackageEmptyChannels(b.Package)
				m[ppkg.Name] = ppkg
			}
			pch, hasCh := ppkg.Channels[b.Channel.Name]
			if !hasCh {
				pch = copyChannelEmptyBundles(b.Channel, ppkg)
				ppkg.Channels[pch.Name] = pch
			}
			if _, hasBundle := pch.Bundles[b.Name]; !hasBundle {
				cb := copyBundle(b, pch, ppkg)
				m.AddBundle(*cb)
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

	return result.ErrorOrNil()
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

func fixReplaces(m model.Model) {
	// Remove unavailable replaces.
	for _, pkg := range m {
		for _, ch := range pkg.Channels {
			for _, b := range ch.Bundles {
				if _, hasReplaces := ch.Bundles[b.Replaces]; !hasReplaces {
					b.Replaces = ""
				}
			}
		}
	}
}
