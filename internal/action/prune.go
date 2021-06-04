package action

import (
	"context"
	"fmt"
	"io"
	"reflect"

	"github.com/sirupsen/logrus"

	"github.com/operator-framework/operator-registry/internal/declcfg"
	"github.com/operator-framework/operator-registry/internal/model"
	"github.com/operator-framework/operator-registry/pkg/image"
	"github.com/operator-framework/operator-registry/pkg/lib/registry"
)

type Prune struct {
	Refs     []string
	Registry image.Registry

	Config     declcfg.PruneConfig
	Keep       bool
	KeepHeads  bool
	Permissive bool

	Logger *logrus.Entry
}

func (p Prune) Run(ctx context.Context) (*declcfg.DeclarativeConfig, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}

	render := Render{Refs: p.Refs, Registry: p.Registry}
	idx := declcfg.NewPackageIndex()
	if err := render.index(ctx, idx); err != nil {
		return nil, err
	}

	toModel, err := p.runIndex(idx)
	if err != nil {
		return nil, err
	}

	prunedConfig := declcfg.ConvertFromModel(toModel)
	return &prunedConfig, nil
}

type PruneRegistry struct {
	Prune
	// Write to W since RegistryPruner's do not have a return value.
	W io.Writer
}

var _ registry.RegistryPruner = PruneRegistry{}

func (p PruneRegistry) PruneFromRegistry(req registry.PruneFromRegistryRequest) error {
	if p.W == nil {
		return fmt.Errorf("writer must be set")
	}

	p.Refs = []string{req.InputDatabase}
	p.Permissive = req.Permissive
	if p.Logger == nil {
		p.Logger = logrus.WithFields(logrus.Fields{
			"db":     req.InputDatabase,
			"pruner": "config",
		})
	} else {
		p.Logger = p.Logger.WithField("db", req.InputDatabase)
	}
	cfg, err := p.Run(context.TODO())
	if err != nil {
		return err
	}
	return declcfg.WriteYAML(*cfg, p.W)
}

func (p Prune) validate() error {
	if reflect.ValueOf(p.Config).IsZero() {
		return fmt.Errorf("prune config must be set")
	}

	return nil
}

func (p Prune) runIndex(idx *declcfg.PackageIndex) (toModel model.Model, err error) {

	toModel, err = declcfg.PruneKeep(idx, p.Config, p.Permissive, p.KeepHeads)
	if err != nil {
		return nil, err
	}

	return toModel, nil
}
