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

	caFile   string
	pullTool string
	skipTLS  bool

	debug  bool
	logger *logrus.Entry
}

func NewCmd() *cobra.Command {
	logger := logrus.New()
	a := diff{
		logger:   logrus.NewEntry(logger),
		pullTool: "none",
	}
	rootCmd := &cobra.Command{
		Use:   "diff --old {index-image|declcfg-dir}... --new {index-image|declcfg-dir}...",
		Short: "diff two sets of indices",
		Long:  "print a diff of latest packages/channels/bundles between old and new references to index sets",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.debug {
				logger.SetLevel(logrus.DebugLevel)
			}
			logger.SetOutput(os.Stderr)
			return nil
		},
		RunE: a.addFunc,
	}

	rootCmd.Flags().StringSliceVar(&a.oldRefs, "old", nil, "old index images or declarative config dirs")
	rootCmd.Flags().StringSliceVar(&a.newRefs, "new", nil, "new index images or declarative config dirs, containing packages/channels/bundles not in old")

	rootCmd.Flags().StringVarP(&a.caFile, "ca-file", "", "", "the root Certificates to use with this command")
	rootCmd.Flags().BoolVar(&a.skipTLS, "skip-tls", false, "disable TLS verification")

	rootCmd.Flags().BoolVar(&a.debug, "debug", false, "enable debug logging")
	return rootCmd
}

func (a *diff) addFunc(cmd *cobra.Command, args []string) error {
	rootCAs, err := certs.RootCAs(a.caFile)
	if err != nil {
		return fmt.Errorf("failed to get root CAs: %v", err)
	}
	reg, err := imgreg.NewRegistry(imgreg.SkipTLS(a.skipTLS), imgreg.WithLog(a.logger), imgreg.WithRootCAs(rootCAs))
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
		Registry: reg,
		OldRefs:  a.oldRefs,
		NewRefs:  a.newRefs,
		Logger:   a.logger,
	}
	cfg, err := diff.Run(ctx)
	if err != nil {
		return err
	}

	return declcfg.WriteYAML(*cfg, os.Stdout)
}
