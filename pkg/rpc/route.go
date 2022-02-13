package rpc

import "github.com/nats-io/nats.go"

type Route struct {
	prefix string
	rpc    *RPC
}

func NewRoute(rpc *RPC, prefix string) *Route {
	return &Route{
		rpc:    rpc,
		prefix: prefix,
	}
}

func (r *Route) Handle(apiPath string, h func(*nats.Msg)) {
	conn := r.rpc.connector.GetClient().GetConnection()
	conn.Subscribe(r.prefix+".", h)
}
