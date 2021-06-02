package index

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"gopkg.in/yaml.v2"

	"github.com/operator-framework/operator-registry/internal/action"
	"github.com/operator-framework/operator-registry/pkg/containertools"
	"github.com/operator-framework/operator-registry/pkg/lib/indexer"
	"github.com/operator-framework/operator-registry/pkg/lib/registry"
)

var isAlphaPrune bool

func init() {
	if os.Getenv("OPM_ALPHA_CONFIG_PRUNER") != "" {
		isAlphaPrune = true
	}
}

func newIndexPruneCmd() *cobra.Command {
	indexCmd := &cobra.Command{
		Use:   "prune",
		Short: "prune an index of all but specified packages",
		Long:  `prune an index of all but specified packages`,

		PreRunE: func(cmd *cobra.Command, args []string) error {
			if debug, _ := cmd.Flags().GetBool("debug"); debug {
				logrus.SetLevel(logrus.DebugLevel)
			}
			return nil
		},

		RunE: runIndexPruneCmdFunc,
	}

	indexCmd.Flags().Bool("debug", false, "enable debug logging")
	indexCmd.Flags().Bool("generate", false, "if enabled, just creates the dockerfile and saves it to local disk")
	indexCmd.Flags().StringP("out-dockerfile", "d", "", "if generating the dockerfile, this flag is used to (optionally) specify a dockerfile name")
	indexCmd.Flags().StringP("from-index", "f", "", "index to prune")
	if err := indexCmd.MarkFlagRequired("from-index"); err != nil {
		logrus.Panic("Failed to set required `from-index` flag for `index prune`")
	}
	indexCmd.Flags().StringP("binary-image", "i", "", "container image for on-image `opm` command")
	indexCmd.Flags().StringP("container-tool", "c", "podman", "tool to interact with container images (save, build, etc.). One of: [docker, podman]")
	indexCmd.Flags().StringP("tag", "t", "", "custom tag for container image being built")
	indexCmd.Flags().Bool("permissive", false, "allow registry load errors")

	if err := indexCmd.Flags().MarkHidden("debug"); err != nil {
		logrus.Panic(err.Error())
	}

	if isAlphaPrune {
		indexCmd.Flags().String("prune-config", "", "prune config directory")
		if err := indexCmd.MarkFlagRequired("prune-config"); err != nil {
			logrus.Panic("Failed to set required `prune-config` flag for `index prune`")
		}
		indexCmd.Flags().Bool("keep", false, "interpret prune configs as an allow-list; "+
			"by default all packages, channels, and bundles in prune configs are removed")
		indexCmd.Flags().Bool("keep-heads", false, "do not remove channel heads; this implies --keep=false")
	} else {
		indexCmd.Flags().StringSliceP("packages", "p", nil, "comma separated list of packages to keep")
		if err := indexCmd.MarkFlagRequired("packages"); err != nil {
			logrus.Panic("Failed to set required `packages` flag for `index prune`")
		}
	}

	return indexCmd

}

func runIndexPruneCmdFunc(cmd *cobra.Command, args []string) error {
	generate, err := cmd.Flags().GetBool("generate")
	if err != nil {
		return err
	}

	outDockerfile, err := cmd.Flags().GetString("out-dockerfile")
	if err != nil {
		return err
	}

	fromIndex, err := cmd.Flags().GetString("from-index")
	if err != nil {
		return err
	}

	binaryImage, err := cmd.Flags().GetString("binary-image")
	if err != nil {
		return err
	}

	containerTool, err := cmd.Flags().GetString("container-tool")
	if err != nil {
		return err
	}

	if containerTool == "none" {
		return fmt.Errorf("none is not a valid container-tool for index prune")
	}

	tag, err := cmd.Flags().GetString("tag")
	if err != nil {
		return err
	}

	permissive, err := cmd.Flags().GetBool("permissive")
	if err != nil {
		return err
	}

	skipTLS, err := cmd.Flags().GetBool("skip-tls")
	if err != nil {
		return err
	}

	logger := logrus.NewEntry(logrus.StandardLogger())

	request := indexer.PruneFromIndexRequest{
		Generate:          generate,
		FromIndex:         fromIndex,
		BinarySourceImage: binaryImage,
		OutDockerfile:     outDockerfile,
		Tag:               tag,
		Permissive:        permissive,
		SkipTLS:           skipTLS,
	}

	var regPruner registry.RegistryPruner
	switch {
	case isAlphaPrune:
		logger = logger.WithFields(logrus.Fields{"pruner": "config"})
		logger.Logger.SetOutput(os.Stderr)
		regPruner, err = configurePruner(cmd.Flags(), logger)
		if err != nil {
			return err
		}
		logger.Info("[alpha config] pruning the index")
	default:
		request.Packages, err = cmd.Flags().GetStringSlice("packages")
		if err != nil {
			return err
		}
		logger = logger.WithFields(logrus.Fields{"packages": request.Packages})
		regPruner = registry.NewRegistryPruner(logger)
		logger.Info("pruning the index")
	}

	ct := containertools.NewContainerTool(containerTool, containertools.PodmanTool)
	indexPruner := indexer.NewIndexPruner(ct, logger, regPruner)

	err = indexPruner.PruneFromIndex(request)
	if err != nil {
		return err
	}

	return nil
}

func configurePruner(fs *pflag.FlagSet, logger *logrus.Entry) (p action.PruneRegistry, err error) {
	p.Logger = logger
	p.W = os.Stdout

	pruneCfgFile, err := fs.GetString("prune-config")
	if err != nil {
		return p, err
	}
	b, err := os.ReadFile(pruneCfgFile)
	if err != nil {
		return p, err
	}
	if err := yaml.Unmarshal(b, &p.Config); err != nil {
		return p, err
	}

	if p.Keep, err = fs.GetBool("keep"); err != nil {
		return p, err
	}
	if p.KeepHeads, err = fs.GetBool("keep-heads"); err != nil {
		return p, err
	}
	if p.KeepHeads {
		p.Keep = false
	}

	return p, nil
}
