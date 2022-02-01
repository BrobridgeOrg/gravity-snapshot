package service

import (
	"github.com/BrobridgeOrg/gravity-snapshot/pkg/configs"
	"github.com/BrobridgeOrg/gravity-snapshot/pkg/connector"
	"github.com/BrobridgeOrg/gravity-snapshot/pkg/snapshot"
	"go.uber.org/zap"
)

type Service struct {
	Config    *configs.Config
	Logger    *zap.Logger
	Connector *connector.Connector
	Snapshot  *snapshot.Snapshot
}

func New() *Service {
	return &Service{}
}
