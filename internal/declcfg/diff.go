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
	Name           string        `json:"name"`
	Channels       []DiffChannel `json:"channels"`
	DefaultChannel string        `json:"defaultChannel"`
}

type DiffChannel struct {
	Name    string   `json:"name"`
	Head    string   `json:"head"`
	Bundles []string `json:"bundles"`
}

type diffModel map[string]diffPackage

type diffPackage struct {
	Name           string
	Channels       map[string]diffChannel
	DefaultChannel string
}

type diffChannel struct {
	Name    string
	Head    string
	Bundles map[string]struct{}
}

func ConvertToDiffConfig(dcfg *DeclarativeConfig) (pcfg DiffConfig, err error) {
	m, err := ConvertToModel(*dcfg)
	if err != nil {
		return pcfg, err
	}
	for _, pkg := range m {
		ppkg := DiffPackage{Name: pkg.Name, DefaultChannel: pkg.DefaultChannel.Name}
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

func convertDiffConfigToDiffModel(dcfg DiffConfig) diffModel {
	dm := diffModel{}
	for _, pkg := range dcfg.Packages {
		dm[pkg.Name] = diffPackage{
			Name:           pkg.Name,
			Channels:       make(map[string]diffChannel, len(pkg.Channels)),
			DefaultChannel: pkg.DefaultChannel,
		}
		for _, ch := range pkg.Channels {
			dm[pkg.Name].Channels[ch.Name] = diffChannel{
				Name:    ch.Name,
				Head:    ch.Head,
				Bundles: make(map[string]struct{}, len(ch.Bundles)),
			}
			for _, b := range ch.Bundles {
				dm[pkg.Name].Channels[ch.Name].Bundles[b] = struct{}{}
			}
		}
	}
	return dm
}

type DiffOptions struct {
	Permissive bool

	Heads   bool
	Deps    bool
	Fill    bool
	Include bool
}

func DiffIndex(idx *PackageIndex, diffCfg DiffConfig, opts DiffOptions) (model.Model, error) {
	return diff(idx, diffCfg, opts.Permissive, opts.Fill, opts.Include, opts.Heads, opts.Deps)
}

func diff(idx *PackageIndex, diffCfg DiffConfig, permissive, fill, include, heads, deps bool) (model.Model, error) {
	dm := convertDiffConfigToDiffModel(diffCfg)

	pkgNames := idx.GetPackageNames()
	pkgNameSet := sets.NewString(pkgNames...)
	outputModel := model.Model{}

	// Add all channel heads from the full catalog to the model.
	if heads {
		for _, pkgName := range pkgNames {
			pkg, err := idx.LoadPackageModel(pkgName)
			if err != nil {
				return nil, err
			}
			diffPkg := copyPackageEmptyChannels(pkg)
			outputModel[diffPkg.Name] = diffPkg
			for _, ch := range pkg.Channels {
				diffCh := copyChannelEmptyBundles(ch, diffPkg)
				diffPkg.Channels[diffCh.Name] = diffCh
				head, err := ch.Head()
				if err != nil {
					return nil, err
				}
				outputModel.AddBundle(*copyBundle(head, diffCh, diffPkg))
			}
		}
	}

	if include {
		// Add all packages, channels, and bundles (package versions) in dm
		// from the full catalog to the model.
		for pkgName, dpkg := range dm {
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
				outputModel[pkgName] = copyPackageEmptyChannels(pkg)
			}
			diffPkg := outputModel[pkgName]
			if len(dpkg.Channels) == 0 {
				for _, ch := range pkg.Channels {
					diffPkg.Channels[ch.Name] = ch
				}
			}
			for _, dch := range dpkg.Channels {
				ch, hasCh := pkg.Channels[dch.Name]
				if !hasCh {
					if !permissive {
						return nil, missingDiffKeyError{keyType: property.TypeChannel, key: dch.Name}
					}
					continue
				}
				if !heads {
					diffPkg.Channels[dch.Name] = copyChannelEmptyBundles(ch, diffPkg)
				}
				diffCh := diffPkg.Channels[dch.Name]
				if len(dch.Bundles) == 0 {
					for _, b := range ch.Bundles {
						outputModel.AddBundle(*b)
					}
				}
				for bName := range dch.Bundles {
					b, hasBundle := ch.Bundles[bName]
					if !hasBundle {
						if !permissive {
							return nil, missingDiffKeyError{keyType: "olm.bundle", key: bName}
						}
						continue
					}
					if heads {
						if _, created := diffCh.Bundles[bName]; created {
							continue
						}
					}
					outputModel.AddBundle(*copyBundle(b, diffCh, diffPkg))
				}
			}

			_, hasDefault := diffPkg.Channels[pkg.DefaultChannel.Name]
			if !hasDefault {
				diffPkg.DefaultChannel = copyChannelEmptyBundles(pkg.DefaultChannel, diffPkg)
			}
		}
	} else if fill {
		if err := fillFromOldHeads(idx, dm, outputModel, heads); err != nil {
			return nil, err
		}
	}

	if deps {
		if err := addDependencies(idx, outputModel); err != nil {
			return nil, err
		}
	}

	fixReplaces(outputModel)

	return outputModel, nil
}

func fillFromOldHeads(idx *PackageIndex, dm diffModel, output model.Model, heads bool) error {
	for _, diffPkg := range dm {
		fmt.Println("in pkg", diffPkg.Name)
		newPkg, err := idx.LoadPackageModel(diffPkg.Name)
		if err != nil {
			return err
		}
		if !heads {
			output[diffPkg.Name] = copyPackageEmptyChannels(newPkg)
		}
		outputPkg := output[diffPkg.Name]
		for _, diffCh := range diffPkg.Channels {
			newCh, hasCh := newPkg.Channels[diffCh.Name]
			if !hasCh {
				continue
			}
			fmt.Println("in ch", diffCh.Name)
			oldHeadName := diffCh.Head
			if oldHeadName == "" {
				return fmt.Errorf("diff model package %q channel %q must specify a head bundle",
					diffPkg.Name, diffCh.Name)
			}
			oldHead, hasHead := newCh.Bundles[oldHeadName]
			if !hasHead {
				return fmt.Errorf("diff model package %q channel %q head %q not found in input model",
					diffPkg.Name, diffCh.Name, oldHeadName)
			}
			newHead, err := newCh.Head()
			if err != nil {
				return err
			}
			fmt.Printf("old head %s, new head %s\n", oldHead.Name, newHead.Name)
			bundleDiff, err := diffChannelBetweenNodes(newCh, oldHead, newHead)
			if err != nil {
				return err
			}
			if !heads {
				outputPkg.Channels[newCh.Name] = copyChannelEmptyBundles(newCh, outputPkg)
			}
			outputCh := outputPkg.Channels[newCh.Name]
			for _, newBundle := range bundleDiff {
				fmt.Println("new bundle:", newBundle.Name)
				output.AddBundle(*copyBundle(newBundle, outputCh, outputPkg))
			}
		}
	}

	return nil
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

func diffChannelBetweenNodes(ch *model.Channel, start, end *model.Bundle) (replacingBundles []*model.Bundle, err error) {
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
				fmt.Println("adding dependency:", cb.Name)
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
