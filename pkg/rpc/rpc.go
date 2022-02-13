package rpc

import (
	"context"
	"fmt"

	"github.com/BrobridgeOrg/gravity-snapshot/pkg/configs"
	"github.com/BrobridgeOrg/gravity-snapshot/pkg/connector"
	"github.com/BrobridgeOrg/gravity-snapshot/pkg/snapshot"
	"github.com/BrobridgeOrg/gravity-snapshot/pkg/view_manager"
	"github.com/nats-io/nats.go"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var logger *zap.Logger

type RPC struct {
	snapshot    *snapshot.Snapshot
	connector   *connector.Connector
	viewManager *view_manager.ViewManager
	routes      *Route
}

func New(lifecycle fx.Lifecycle, config *configs.Config, l *zap.Logger, c *connector.Connector, s *snapshot.Snapshot, vm *view_manager.ViewManager) *RPC {

	logger = l.Named("RPC")

	rpc := &RPC{
		snapshot:    s,
		connector:   c,
		viewManager: vm,
	}

	lifecycle.Append(
		fx.Hook{
			OnStart: func(context.Context) error {

				// Preparing prefix
				prefix := fmt.Sprintf("$GRAVITY.%s.API.SNAPSHOT", rpc.connector.GetDomain())
				rpc.routes = NewRoute(rpc, prefix)

				logger.Info("Initializing RPC",
					zap.String("prefix", prefix),
				)
				return rpc.register()
			},
			OnStop: func(ctx context.Context) error {
				return nil
			},
		},
	)

	return rpc
}

func (rpc *RPC) register() error {
	rpc.routes.Handle("VIEW.CREATE", rpc.createSnapshotView)
	rpc.routes.Handle("VIEW.DELETE", rpc.deleteSnapshotView)
	rpc.routes.Handle("VIEW.PULL", rpc.pullSnapshotView)

	return nil
}

func (rpc *RPC) assertStream(streamName string) error {

	// Preparing JetStream
	js, err := rpc.connector.GetClient().GetJetStream()
	if err != nil {
		return err
	}

	// Check if the stream already exists
	stream, err := js.StreamInfo(streamName)
	if err != nil {
		logger.Warn(err.Error())
	}

	// New stream
	if stream == nil {

		subject := fmt.Sprintf("%s.*", streamName)

		// Initializing stream
		logger.Info("Creating stream for snapshot view...",
			zap.String("stream", streamName),
			zap.String("subject", subject),
		)

		_, err := js.AddStream(&nats.StreamConfig{
			Name:        streamName,
			Description: "Gravity snapshot view",
			Subjects: []string{
				subject,
			},
		})

		if err != nil {
			return err
		}
	}

	return nil
}
