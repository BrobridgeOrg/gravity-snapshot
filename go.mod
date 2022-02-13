module github.com/BrobridgeOrg/gravity-snapshot

go 1.15

require (
	github.com/BrobridgeOrg/EventStore v0.0.22
	github.com/BrobridgeOrg/gravity-sdk v0.0.50
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.1.2
	github.com/nats-io/nats-server v1.4.1
	github.com/nats-io/nats-streaming-server v0.24.1
	github.com/nats-io/nats.go v1.13.1-0.20220121202836-972a071d373d
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.3.0
	github.com/spf13/viper v1.10.1
	go.uber.org/fx v1.16.0
	go.uber.org/zap v1.17.0
	google.golang.org/protobuf v1.27.1
)

replace github.com/BrobridgeOrg/gravity-sdk => ../gravity-sdk
