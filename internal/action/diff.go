package action

import (
	"context"
	"fmt"

	"github.com/operator-framework/operator-registry/internal/declcfg"
	"github.com/operator-framework/operator-registry/internal/model"
	"github.com/operator-framework/operator-registry/pkg/image"
	"github.com/sirupsen/logrus"
)

type Diff struct {
	Registry image.Registry

	OldRefs []string
	NewRefs []string

	WithDeps   bool
	Permissive bool

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

	newRender := Render{Refs: a.NewRefs, Registry: a.Registry}
	newCfg, err := newRender.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("error rendering new refs: %v", err)
	}

	diffModel, err := runModel(*oldCfg, *newCfg, a.WithDeps)
	if err != nil {
		return nil, err
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

func runModel(oldCfg, newCfg declcfg.DeclarativeConfig, withDeps bool) (model.Model, error) {
	oldModel, err := declcfg.ConvertToModel(oldCfg)
	if err != nil {
		return nil, fmt.Errorf("error converting old cfg to model: %v", err)
	}
	newModel, err := declcfg.ConvertToModel(newCfg)
	if err != nil {
		return nil, fmt.Errorf("error converting new cfg to model: %v", err)
	}

	diff, err := declcfg.DiffFromOldChannelHeads(oldModel, newModel, withDeps)
	if err != nil {
		return nil, fmt.Errorf("error generating diff: %v", err)
	}

	return diff, nil
}
