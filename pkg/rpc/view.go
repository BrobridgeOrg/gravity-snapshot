package rpc

import (
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/BrobridgeOrg/gravity-snapshot/pkg/view_manager"
	"github.com/nats-io/nats.go"
)

type CreateSnapshotViewRequest struct {
	Subscriber string `json:"subscriber"`
	Collection string `json:"collection"`
}

type CreateSnapshotViewReply struct {
	ID         string    `json:"id"`
	Subscriber string    `json:"subscriber"`
	Collection string    `json:"collection"`
	CreatedAt  time.Time `json:"createAt"`
}

type DeleteSnapshotViewRequest struct {
	ID string `json:"id"`
}

type DeleteSnapshotViewReply struct {
	ID string `json:"id"`
}

type PullSnapshotViewRequest struct {
	ID           string `json:"id"`
	LastKey      string `json:"lastKey"`
	AfterLastKey bool   `json:"afterLastKey"`
}

type PullSnapshotViewReply struct {
	ID    string `json:"id"`
	Count int    `json:"count"`
}

type ErrorReply struct {
	Error *Error `json:"error"`
}

func (rpc *RPC) createSnapshotView(msg *nats.Msg) {

	// Parsing request
	var req CreateSnapshotViewRequest
	err := json.Unmarshal(msg.Data, &req)
	if err != nil {
		logger.Error(err.Error())
		return
	}

	// Create new view
	view, err := rpc.viewManager.CreateView(
		view_manager.WithSubscriber(req.Subscriber),
		view_manager.WithCollection(req.Collection),
	)
	if err != nil {
		logger.Error(err.Error())
		return
	}

	resp := &CreateSnapshotViewReply{
		ID:         view.ID,
		Subscriber: view.Subscriber,
		Collection: view.Collection,
		CreatedAt:  view.CreatedAt,
	}

	data, _ := json.Marshal(resp)

	// Response
	err = msg.Respond(data)
	if err != nil {
		logger.Error(err.Error())
		return
	}
}

func (rpc *RPC) deleteSnapshotView(msg *nats.Msg) {

	// Parsing request
	var req DeleteSnapshotViewRequest
	err := json.Unmarshal(msg.Data, &req)
	if err != nil {
		logger.Error(err.Error())
		return
	}

	// Delete view
	err = rpc.viewManager.DeleteView(req.ID)
	if err != nil {
		logger.Error(err.Error())
		return
	}

	resp := &DeleteSnapshotViewReply{
		ID: req.ID,
	}

	data, _ := json.Marshal(resp)

	// Response
	err = msg.Respond(data)
	if err != nil {
		logger.Error(err.Error())
		return
	}
}

func (rpc *RPC) pullSnapshotView(msg *nats.Msg) {

	// Parsing request
	var req PullSnapshotViewRequest
	err := json.Unmarshal(msg.Data, &req)
	if err != nil {
		logger.Error(err.Error())
		return
	}

	// Get specific view
	view, err := rpc.viewManager.GetView(req.ID)
	if err != nil {
		logger.Error(err.Error())
		return
	}

	if view == nil {
		e := &ErrorReply{
			Error: NotFoundViewErr(),
		}

		data, _ := json.Marshal(e)

		// Response
		err = msg.Respond(data)
		if err != nil {
			logger.Error(err.Error())
		}

		return
	}

	// Decode key from base64
	lastKey, err := base64.StdEncoding.DecodeString(req.LastKey)
	if err != nil {
		logger.Error(err.Error())
		return
	}

	count, err := view.Fetch(lastKey, req.AfterLastKey)
	if err != nil {
		logger.Error(err.Error())
		return
	}

	resp := &PullSnapshotViewReply{
		ID:    req.ID,
		Count: count,
	}

	data, _ := json.Marshal(resp)

	// Response
	err = msg.Respond(data)
	if err != nil {
		logger.Error(err.Error())
		return
	}
}
