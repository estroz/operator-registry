package action

import (
	"context"
	"fmt"
	"os"
	"reflect"

	"github.com/operator-framework/operator-registry/internal/declcfg"
	"github.com/operator-framework/operator-registry/internal/model"
	"github.com/operator-framework/operator-registry/pkg/image"
	"github.com/operator-framework/operator-registry/pkg/lib/registry"
	"github.com/operator-framework/operator-registry/pkg/sqlite"
	"github.com/sirupsen/logrus"
)

type Prune struct {
	Refs     []string
	Registry image.Registry

	Config     declcfg.DeclarativeConfig
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
}

var _ registry.RegistryPruner = PruneRegistry{}

func (p PruneRegistry) PruneFromRegistry(req registry.PruneFromRegistryRequest) error {
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
	return p.Run(context.TODO())
}

func (p PruneRegistry) Run(ctx context.Context) error {
	if err := p.validate(); err != nil {
		return err
	}

	render := Render{Refs: p.Refs, Registry: p.Registry}
	cfg, err := render.Run(ctx)
	if err != nil {
		return err
	}

	toModel, err := p.runModel(*cfg)
	if err != nil {
		return err
	}

	store, closeStore, err := p.newStore(ctx, p.Refs[0])
	defer closeStore()
	if err != nil {
		return err
	}

	if err = sqlite.FromModel(ctx, toModel, store); err != nil {
		return err
	}

	return nil
}

func (p Prune) validate() error {
	if reflect.ValueOf(p.Config).IsZero() {
		return fmt.Errorf("prune config must be set")
	}

	return nil
}

func (p Prune) runModel(cfg declcfg.DeclarativeConfig) (toModel model.Model, err error) {
	fromModel, err := declcfg.ConvertToModel(p.Config)
	if err != nil {
		return nil, err
	}
	pruneModel, err := declcfg.ConvertToModel(cfg)
	if err != nil {
		return nil, err
	}

	if p.Keep {
		toModel, err = model.PruneKeep(fromModel, pruneModel, p.Permissive, p.KeepHeads)
	} else {
		toModel, err = model.PruneRemove(fromModel, pruneModel, p.Permissive)
	}
	if err != nil {
		return nil, err
	}

	return toModel, nil
}

func (p PruneRegistry) newStore(ctx context.Context, dbPath string) (store sqlite.MigratableLoader, closeStore func(), err error) {
	closeStore = func() {}

	// Zero the db file so we can freshly add the pruned index.
	//
	// QUESTION: is the assumption that a db produced by adding bundles
	// in random order consistent with a db pruned by removal correct?
	if err := os.Truncate(dbPath, 0); err != nil {
		return nil, nil, err
	}

	db, err := sqlite.Open(dbPath)
	if err != nil {
		return nil, nil, err
	}
	closeStore = func() {
		if cerr := db.Close(); cerr != nil {
			p.Logger.Error(cerr)
		}
	}

	dbLoader, err := sqlite.NewSQLLiteLoader(db)
	if err != nil {
		return nil, nil, err
	}
	if err := dbLoader.Migrate(ctx); err != nil {
		return nil, nil, err
	}

	return dbLoader, closeStore, nil
}
