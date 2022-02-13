package snapshot

import (
	"context"
	"fmt"

	eventstore "github.com/BrobridgeOrg/EventStore"
	"github.com/BrobridgeOrg/gravity-snapshot/pkg/configs"
	"github.com/BrobridgeOrg/gravity-snapshot/pkg/connector"
	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var logger *zap.Logger

type Snapshot struct {
	config     *configs.Config
	connector  *connector.Connector
	watcher    *CollectionWatcher
	eventstore *eventstore.EventStore
	store      *eventstore.Store
	handler    *SnapshotHandler
}

func New(lifecycle fx.Lifecycle, config *configs.Config, l *zap.Logger, c *connector.Connector) *Snapshot {

	logger = l.Named("Snapshot")

	d := &Snapshot{
		config:    config,
		connector: c,
		handler:   NewSnapshotHandler(),
	}

	// Initializing event watcher
	d.watcher = NewCollectionWatcher(d.connector.GetClient(), d.connector.GetDomain())
	d.registerCollections()

	lifecycle.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				return d.Run()
			},
			OnStop: func(ctx context.Context) error {
				return nil
			},
		},
	)

	return d
}

func (d *Snapshot) initializeStore() error {

	options := eventstore.NewOptions()
	options.DatabasePath = viper.GetString("datastore.path")
	options.EnabledSnapshot = true

	// Snapshot options
	viper.SetDefault("snapshot.workerCount", 8)
	viper.SetDefault("snapshot.workerBufferSize", 102400)
	options.SnapshotOptions.WorkerCount = viper.GetInt32("snapshot.workerCount")
	options.SnapshotOptions.BufferSize = viper.GetInt("snapshot.workerBufferSize")

	logger.Info("Initialize store",
		zap.String("databasePath", options.DatabasePath),
		zap.Int32("storeWorkerCount", options.SnapshotOptions.WorkerCount),
		zap.Int("storeBufferSize", options.SnapshotOptions.BufferSize),
	)

	// Initialize event store
	es, err := eventstore.CreateEventStore(options)
	if err != nil {
		return err
	}

	d.eventstore = es

	// Setup snapshot
	es.SetSnapshotHandler(func(request *eventstore.SnapshotRequest) error {
		meta := map[string]interface{}{
			"revision": request.Sequence,
		}
		return d.handler.handle(meta, request)
	})

	return nil
}

func (d *Snapshot) registerCollections() error {

	// Default events
	for _, e := range d.config.Collections {
		logger.Info(fmt.Sprintf("Regiserted collection: %s", e))
		d.watcher.RegisterCollection(e)
	}

	return nil
}

func (d *Snapshot) Run() error {

	d.watcher.Watch(func(collection string, partition uint64, msg *nats.Msg) {

		meta, err := msg.Metadata()
		if err != nil {
			return
		}

		// take snapshot
		d.eventstore.TakeSnapshot(d.store, meta.Sequence.Consumer, msg.Data)

	})

	return nil
}
