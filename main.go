package main

import (
	"os"

	"github.com/BrobridgeOrg/gravity-snapshot/pkg/configs"
	"github.com/BrobridgeOrg/gravity-snapshot/pkg/connector"
	"github.com/BrobridgeOrg/gravity-snapshot/pkg/logger"
	"github.com/BrobridgeOrg/gravity-snapshot/pkg/rpc"
	"github.com/BrobridgeOrg/gravity-snapshot/pkg/service"
	"github.com/BrobridgeOrg/gravity-snapshot/pkg/snapshot"
	"github.com/BrobridgeOrg/gravity-snapshot/pkg/view_manager"
	"github.com/spf13/cobra"

	"go.uber.org/fx"
)

var config *configs.Config
var collections []string

var rootCmd = &cobra.Command{
	Use:   "gravity-snapshot",
	Short: "Gravity Component to store data snapshot of collection",
	Long: `gravity-snapshot a component to manage collection snapshot.
This application can be used to merge incoming events and store the latest data state`,
	RunE: func(cmd *cobra.Command, args []string) error {

		if err := run(); err != nil {
			return err
		}
		return nil
	},
}

func init() {
	config = configs.GetConfig()

	rootCmd.Flags().StringSliceVar(&collections, "collections", []string{}, "Specify collections for watching")
}

func main() {

	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func run() error {

	var serv *service.Service

	config.AddCollections(collections)

	fx.New(
		fx.Supply(config),
		fx.Provide(
			logger.GetLogger,
			connector.New,
			snapshot.New,
			view_manager.New,
		),
		fx.Invoke(rpc.New),
		fx.Populate(&serv),
	).Run()

	return nil
}
