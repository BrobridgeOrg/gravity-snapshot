package snapshot

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/BrobridgeOrg/gravity-sdk/core"
	"github.com/nats-io/nats.go"
)

type Collection struct {
	client     *core.Client
	domain     string
	partitions []uint64
	name       string
}

func NewCollection() *Collection {
	return &Collection{
		partitions: make([]uint64, 0),
	}
}

func (c *Collection) assertStream(streamName string) error {

	// Preparing JetStream
	js, err := c.client.GetJetStream()
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
		logger.Info("Creating stream...",
			zap.String("stream", streamName),
			zap.String("subject", subject),
		)

		_, err := js.AddStream(&nats.StreamConfig{
			Name:        streamName,
			Description: "Gravity collection event store",
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

func (c *Collection) watch(partition uint64, fn func(string, uint64, *nats.Msg)) error {

	streamName := fmt.Sprintf("GRAVITY-%s.COLLECTION.%s", c.domain, c.name)
	subject := fmt.Sprintf("%s.%d.EVENT.*", streamName, partition)
	durableName := fmt.Sprintf("%s-%s-%d-SNAPSHOT", c.domain, c.name, partition)

	// Preparing JetStream
	js, err := c.client.GetJetStream()
	if err != nil {
		return err
	}

	logger.Info("Watching collection",
		zap.String("stream", streamName),
	)

	// Subscribe to stream
	_, err = js.Subscribe(subject, func(msg *nats.Msg) {
		fn(c.name, partition, msg)
	}, nats.DeliverNew(), nats.AckAll(), nats.Durable(durableName))
	if err != nil {
		return err
	}

	return nil
}

func (c *Collection) Watch(fn func(string, uint64, *nats.Msg)) error {

	logger.Info("Subscribing to collection",
		zap.String("name", c.name),
	)

	streamName := fmt.Sprintf("GRAVITY-%s.COLLECTION.%s", c.domain, c.name)

	err := c.assertStream(streamName)
	if err != nil {
		return err
	}

	for _, partition := range c.partitions {
		c.watch(partition, fn)
	}

	return nil
}

type CollectionWatcher struct {
	client      *core.Client
	domain      string
	collections map[string]*Collection
}

func NewCollectionWatcher(client *core.Client, domain string) *CollectionWatcher {
	return &CollectionWatcher{
		client:      client,
		domain:      domain,
		collections: make(map[string]*Collection),
	}
}

func (ew *CollectionWatcher) RegisterCollection(name string) *Collection {

	if e, ok := ew.collections[name]; ok {
		return e
	}

	e := NewCollection()
	e.name = name

	subject := fmt.Sprintf("GRAVITY-%s.COLLECTION.%s", ew.domain, name)

	ew.collections[subject] = e

	return e
}

func (ew *CollectionWatcher) UnregisterCollection(name string) {

	if _, ok := ew.collections[name]; !ok {
		return
	}

	delete(ew.collections, name)
}

func (ew *CollectionWatcher) GetCollection(name string) *Collection {

	if v, ok := ew.collections[name]; ok {
		return v
	}

	return nil
}

func (ew *CollectionWatcher) Watch(fn func(string, uint64, *nats.Msg)) error {

	logger.Info("Starting watch collections...")

	for _, collection := range ew.collections {

		err := collection.Watch(fn)
		if err != nil {
			logger.Warn(err.Error())
			continue
		}

		logger.Info(fmt.Sprintf("    Watched %s", collection.name))
	}

	return nil
}
