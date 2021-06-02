package action

import (
	"context"
	"fmt"
	"io"
	"reflect"

	"github.com/operator-framework/operator-registry/internal/declcfg"
	"github.com/operator-framework/operator-registry/internal/model"
	"github.com/operator-framework/operator-registry/pkg/image"
	"github.com/operator-framework/operator-registry/pkg/lib/registry"
	"github.com/sirupsen/logrus"
)

type Prune struct {
	Refs     []string
	Registry image.Registry

	Config     PruneConfig
	Keep       bool
	KeepHeads  bool
	Permissive bool

	Logger *logrus.Entry
}

type PruneConfig []Package

type Package struct {
	Name     string
	Channels []Channel
}

type Channel struct {
	Name    string
	Bundles []string
}

func (p Prune) Run(ctx context.Context) (*declcfg.DeclarativeConfig, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}

	render := Render{Refs: p.Refs, Registry: p.Registry}
	cfg, err := render.Run(ctx)
	if err != nil {
		return nil, err
	}

	toModel, err := p.runModel(*cfg)
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

	// TODO: Render does not recognize bare db files yet so this does not work.
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

func (p Prune) runModel(cfg declcfg.DeclarativeConfig) (toModel model.Model, err error) {
	fromModel, err := declcfg.ConvertToModel(cfg)
	if err != nil {
		return nil, err
	}

	pruneCfg := make(map[string]map[string]map[string]struct{}, len(p.Config))
	for _, pkg := range p.Config {
		pruneCfg[pkg.Name] = make(map[string]map[string]struct{}, len(pkg.Channels))
		for _, ch := range pkg.Channels {
			pruneCfg[pkg.Name][ch.Name] = make(map[string]struct{}, len(ch.Bundles))
			for _, b := range ch.Bundles {
				pruneCfg[pkg.Name][ch.Name][b] = struct{}{}
			}
		}
	}

	toModel, err = declcfg.PruneKeep(fromModel, pruneCfg, p.Permissive, p.KeepHeads)
	if err != nil {
		return nil, err
	}

	return toModel, nil
}
