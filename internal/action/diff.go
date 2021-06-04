package action

import (
	"context"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"sigs.k8s.io/yaml"

	"github.com/operator-framework/operator-registry/internal/declcfg"
	"github.com/operator-framework/operator-registry/pkg/image"
)

type Diff struct {
	Registry image.Registry

	OldRefs []string
	NewRefs []string

	WithDeps   bool
	WithHeads  bool
	Permissive bool

	Logger *logrus.Entry
}

func (a Diff) Run(ctx context.Context) (*declcfg.DeclarativeConfig, error) {
	if err := a.validate(); err != nil {
		return nil, err
	}

	newRender := Render{Refs: a.NewRefs, Registry: a.Registry}
	idx := declcfg.NewPackageIndex()
	if err := newRender.index(ctx, idx); err != nil {
		return nil, err
	}

	oldRender := Render{Registry: a.Registry}
	var pruneCfgs []declcfg.PruneConfig
	for _, ref := range a.OldRefs {
		if info, err := os.Stat(ref); err != nil || info.IsDir() {
			oldRender.Refs = append(oldRender.Refs, ref)
			continue
		}
		b, err := os.ReadFile(ref)
		if err != nil {
			return nil, err
		}
		var pruneCfg declcfg.PruneConfig
		if err := yaml.UnmarshalStrict(b, &pruneCfg); err != nil {
			oldRender.Refs = append(oldRender.Refs, ref)
			continue
		}
		pruneCfgs = append(pruneCfgs, pruneCfg)
	}
	if len(oldRender.Refs) != 0 {
		oldCfg, err := oldRender.Run(ctx)
		if err != nil {
			return nil, fmt.Errorf("error rendering old refs: %v", err)
		}
		pruneCfg, err := declcfg.ConfigToPruneConfig(oldCfg)
		if err != nil {
			return nil, err
		}
		pruneCfgs = append(pruneCfgs, pruneCfg)
	}

	pruneCfg := combinePruneConfigs(pruneCfgs)
	diffModel, err := declcfg.PruneRemove(idx, pruneCfg, a.Permissive, a.WithHeads, a.WithDeps)
	if err != nil {
		return nil, fmt.Errorf("error generating diff: %v", err)
	}

	diffCfg := declcfg.ConvertFromModel(diffModel)
	return &diffCfg, nil
}

func (p Diff) validate() error {
	if len(p.OldRefs) == 0 {
		return fmt.Errorf("no old refs to diff")
	}
	if len(p.NewRefs) == 0 {
		return fmt.Errorf("no new refs to diff")
	}
	return nil
}

// TODO: validate all prune configs, since they can contain duplicates.
func combinePruneConfigs(cfgs []declcfg.PruneConfig) (finalCfg declcfg.PruneConfig) {
	for _, cfg := range cfgs {
		finalCfg.Packages = append(finalCfg.Packages, cfg.Packages...)
	}
	return finalCfg
}
