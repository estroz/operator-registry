package action

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/operator-framework/operator-registry/internal/declcfg"
	"github.com/operator-framework/operator-registry/pkg/image"
)

type Diff struct {
	Registry image.Registry

	NewRefs []string
	OldRefs []string

	Deps bool

	Logger *logrus.Entry
}

func (a Diff) Run(ctx context.Context) (*declcfg.DeclarativeConfig, error) {
	if err := a.validate(); err != nil {
		return nil, err
	}

	oldRender := Render{Refs: a.OldRefs, Registry: a.Registry}
	oldCfg, err := oldRender.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("error rendering old refs: %v", err)
	}
	oldModel, err := declcfg.ConvertToModel(*oldCfg)
	if err != nil {
		return nil, fmt.Errorf("error converting old delcarative config to model: %v", err)
	}

	newRender := Render{Refs: a.NewRefs, Registry: a.Registry}
	newCfg, err := newRender.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("error rendering new refs: %v", err)
	}
	newModel, err := declcfg.ConvertToModel(*newCfg)
	if err != nil {
		return nil, fmt.Errorf("error converting new delcarative config to model: %v", err)
	}

	opts := declcfg.DiffOptions{
		Deps: a.Deps,
	}
	diffModel, err := declcfg.DiffFromHeads(oldModel, newModel, opts)
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
