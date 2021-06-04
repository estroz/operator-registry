package declcfg

import (
	"reflect"

	"github.com/mitchellh/hashstructure/v2"

	"github.com/operator-framework/operator-registry/internal/model"
)

// Diff returns a Model containing everything in newModel not in oldModel,
// and all bundles that exist in oldModel but are different in newModel.
// Side effect: this function modifies then returns newModel.
func Diff(oldModel, newModel model.Model) (model.Model, error) {

	// TODO: loading both oldModel and newModel into memory may
	// exceed process/hardware limits. Instead, store models on-disk then
	// load by package.
	for _, newPkg := range newModel {
		oldPkg, oldHasPkg := oldModel[newPkg.Name]
		if !oldHasPkg {
			continue
		}
		if err := diffPackages(oldPkg, newPkg); err != nil {
			return nil, err
		}
		if len(newPkg.Channels) == 0 {
			delete(newModel, newPkg.Name)
		}
	}

	return newModel, nil
}

// diffPackages removes any bundles and channels from newPkg that
// are in oldPkg, but not those that differ in any way.
func diffPackages(oldPkg, newPkg *model.Package) error {
	for _, newCh := range newPkg.Channels {
		oldCh, oldHasCh := oldPkg.Channels[newCh.Name]
		if !oldHasCh {
			continue
		}
		for _, newBundle := range newCh.Bundles {
			oldBundle, oldHasBundle := oldCh.Bundles[newBundle.Name]
			if !oldHasBundle {
				continue
			}
			equal, err := bundlesEqual(oldBundle, newBundle)
			if err != nil {
				return err
			}
			if equal {
				delete(newCh.Bundles, oldBundle.Name)
			}
		}
		if len(newCh.Bundles) == 0 {
			delete(newPkg.Channels, newCh.Name)
		}
	}
	return nil
}

// bundlesEqual computes then compares the hashes of b1 and b2 for equality.
func bundlesEqual(b1, b2 *model.Bundle) (bool, error) {
	// Use a declarative config bundle type to avoid infinite recursion.
	dcBundle1 := convertFromModelBundle(b1)
	dcBundle2 := convertFromModelBundle(b2)

	hash1, err := hashstructure.Hash(dcBundle1, hashstructure.FormatV2, nil)
	if err != nil {
		return false, err
	}
	hash2, err := hashstructure.Hash(dcBundle2, hashstructure.FormatV2, nil)
	if err != nil {
		return false, err
	}
	// CsvJSON and Objects are ignored by Hash, so they must be compared separately.
	return hash1 == hash2 && b1.CsvJSON == b2.CsvJSON && reflect.DeepEqual(b1.Objects, b2.Objects), nil
}

// Does not handle
func convertFromModelBundle(b *model.Bundle) Bundle {
	return Bundle{
		Schema:        schemaBundle,
		Name:          b.Name,
		Package:       b.Package.Name,
		Image:         b.Image,
		RelatedImages: modelRelatedImagesToRelatedImages(b.RelatedImages),
		CsvJSON:       b.CsvJSON,
		Objects:       b.Objects,
		Properties:    b.Properties,
	}
}
