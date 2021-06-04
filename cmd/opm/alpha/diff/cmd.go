package diff

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/operator-framework/operator-registry/internal/action"
	"github.com/operator-framework/operator-registry/internal/declcfg"
	imgreg "github.com/operator-framework/operator-registry/pkg/image/containerdregistry"
	"github.com/operator-framework/operator-registry/pkg/lib/certs"
)

const (
	retryInterval = time.Second * 5
	timeout       = time.Minute * 1
)

type diff struct {
	oldRefs []string
	newRefs []string

	fill       bool
	heads      bool
	deps       bool
	permissive bool

	caFile   string
	pullTool string

	debug  bool
	logger *logrus.Entry
}

func NewCmd() *cobra.Command {
	a := diff{
		logger:   logrus.NewEntry(logrus.New()),
		pullTool: "none",
	}
	rootCmd := &cobra.Command{
		Use:  "diff --old { diff-config | render-target }... --new { render-target }...",
		Long: "Generate a diff of latest packages/channels/versions between old and new references to index sets",
		Args: cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.debug {
				a.logger.Logger.SetLevel(logrus.DebugLevel)
			}
			a.logger.Logger.SetOutput(os.Stderr)
			return nil
		},
		RunE: a.addFunc,
	}

	rootCmd.Flags().StringSliceVar(&a.oldRefs, "old", nil, "a diff config or render targets containing the base catalog or current channel heads")
	rootCmd.Flags().StringSliceVar(&a.newRefs, "new", nil, "render targets containing the latest catalog")
	rootCmd.Flags().BoolVar(&a.fill, "fill", false,
		"Produce a catalog containing only packages, channels, and versions specified within --old "+
			"using the full corresponding objects in --new. Default behavior (false) is to produce "+
			"an exact diff between --old and --new. --heads and --deps are still respected")
	rootCmd.Flags().BoolVar(&a.deps, "deps", true, "include all new dependencies in the diff")
	rootCmd.Flags().BoolVar(&a.heads, "heads", true, "include all new heads in the diff")
	rootCmd.Flags().BoolVar(&a.permissive, "permissive", false, "Do not return errors if --old contains content not in --new")

	rootCmd.Flags().StringVarP(&a.caFile, "ca-file", "", "", "the root Certificates to use with this command")

	rootCmd.Flags().BoolVar(&a.debug, "debug", false, "enable debug logging")
	return rootCmd
}

func (a *diff) addFunc(cmd *cobra.Command, args []string) error {
	skipTLS, err := cmd.PersistentFlags().GetBool("skip-tls")
	if err != nil {
		a.logger.Fatal(err)
	}

	rootCAs, err := certs.RootCAs(a.caFile)
	if err != nil {
		return fmt.Errorf("failed to get root CAs: %v", err)
	}
	reg, err := imgreg.NewRegistry(imgreg.SkipTLS(skipTLS), imgreg.WithLog(a.logger), imgreg.WithRootCAs(rootCAs))
	if err != nil {
		return err
	}
	defer func() {
		if err := reg.Destroy(); err != nil {
			a.logger.Errorf("error destroying local cache: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()

	diff := action.Diff{
		Registry:   reg,
		OldRefs:    a.oldRefs,
		NewRefs:    a.newRefs,
		Fill:       a.fill,
		Deps:       a.deps,
		Heads:      a.heads,
		Permissive: a.permissive,
		Logger:     a.logger,
	}
	cfg, err := diff.Run(ctx)
	if err != nil {
		return err
	}

	return declcfg.WriteYAML(*cfg, os.Stdout)
}
