package view_manager

import (
	"fmt"

	"github.com/BrobridgeOrg/gravity-snapshot/pkg/configs"
	"github.com/BrobridgeOrg/gravity-snapshot/pkg/connector"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

var logger *zap.Logger

type ViewManager struct {
	config    *configs.Config
	connector *connector.Connector
	views     map[string]*View
}

func New(config *configs.Config, l *zap.Logger, c *connector.Connector) *ViewManager {

	logger = l.Named("ViewManager")

	vm := &ViewManager{
		config:    config,
		connector: c,
		views:     make(map[string]*View),
	}

	return vm
}

func (vm *ViewManager) assertStream(viewID string) error {

	streamName := fmt.Sprintf("GRAVITY.%s.SNAPSHOT.VIEW.%s", vm.connector.GetDomain(), viewID)

	// Preparing JetStream
	nc := vm.connector.GetClient().GetConnection()
	js, err := nc.JetStream()
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

		subject := fmt.Sprintf("%s", streamName)

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

func (vm *ViewManager) CreateView(opts ...func(*ViewManager, *View)) (*View, error) {

	view := NewView()

	// Generate view ID
	id, _ := uuid.NewUUID()
	view.ID = id.String()

	for _, opt := range opts {
		opt(vm, view)
	}

	// TODO: Register on distributed data store
	vm.views[id.String()] = view

	return view, nil
}

func (vm *ViewManager) DeleteView(id string) error {

	// TODO: delete from distributed data store
	delete(vm.views, id)

	return nil
}

func (vm *ViewManager) GetView(id string) (*View, error) {

	// TODO: get from distributed data store
	v, ok := vm.views[id]
	if !ok {
		return nil, nil
	}

	//return nil, errors.New(fmt.Sprintf("No such view: %s", id))

	return v, nil
}

func WithSubscriber(subscriberID string) func(vm *ViewManager, view *View) {
	return func(vm *ViewManager, view *View) {
		view.Subscriber = subscriberID
	}
}

func WithCollection(collection string) func(vm *ViewManager, view *View) {
	return func(vm *ViewManager, view *View) {
		view.Collection = collection
	}
}
