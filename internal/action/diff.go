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

	NewRefs []string
	OldRefs []string

	Fill       bool
	Deps       bool
	Heads      bool
	Permissive bool

	Logger *logrus.Entry
}

func (a Diff) Run(ctx context.Context) (*declcfg.DeclarativeConfig, error) {
	if err := a.validate(); err != nil {
		return nil, err
	}

	newRender := Render{Refs: a.NewRefs, Registry: a.Registry}
	idx := declcfg.NewPackageIndex()
	defer func() {
		if cerr := idx.Cleanup(); cerr != nil {
			a.Logger.Error(cerr)
		}
	}()
	if err := newRender.index(ctx, idx); err != nil {
		return nil, err
	}

	oldRender := Render{Registry: a.Registry}
	var diffCfgs []declcfg.DiffConfig
	for _, ref := range a.OldRefs {
		if info, err := os.Stat(ref); err != nil || info.IsDir() {
			fmt.Println("adding old", ref)
			oldRender.Refs = append(oldRender.Refs, ref)
			continue
		}
		b, err := os.ReadFile(ref)
		if err != nil {
			return nil, err
		}
		var diffCfg declcfg.DiffConfig
		if err := yaml.UnmarshalStrict(b, &diffCfg); err != nil {
			oldRender.Refs = append(oldRender.Refs, ref)
			continue
		}
		diffCfgs = append(diffCfgs, diffCfg)
	}
	if len(oldRender.Refs) != 0 {
		oldCfg, err := oldRender.Run(ctx)
		if err != nil {
			return nil, fmt.Errorf("error rendering old refs: %v", err)
		}
		diffCfg, err := declcfg.ConvertToDiffConfig(oldCfg)
		if err != nil {
			return nil, err
		}
		diffCfgs = append(diffCfgs, diffCfg)
	}

	diffCfg := combineDiffConfigs(diffCfgs)
	fmt.Println("diff config:", diffCfg)
	opts := declcfg.DiffOptions{
		Fill:       a.Fill,
		Permissive: a.Permissive,
		Heads:      a.Heads,
		Deps:       a.Deps,
	}
	diffModel, err := declcfg.DiffIndex(idx, diffCfg, opts)
	if err != nil {
		return nil, fmt.Errorf("error generating diff: %v", err)
	}

	cfg := declcfg.ConvertFromModel(diffModel)
	return &cfg, nil
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

// TODO: validate all DiffConfigs, since they can contain duplicates.
func combineDiffConfigs(cfgs []declcfg.DiffConfig) (finalCfg declcfg.DiffConfig) {
	for _, cfg := range cfgs {
		finalCfg.Packages = append(finalCfg.Packages, cfg.Packages...)
	}
	return finalCfg
}
