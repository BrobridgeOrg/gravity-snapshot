package view_manager

import (
	"time"
)

type View struct {
	vm *ViewManager

	ID         string
	Subscriber string
	Collection string
	CreatedAt  time.Time
}

func NewView() *View {
	return &View{
		CreatedAt: time.Now(),
	}
}

func (view *View) Fetch(lastKey []byte, afterLastKey bool) (int, error) {

	err := view.vm.assertStream(view.ID)
	if err != nil {
		return 0, err
	}

	// TODO: Fetch data from store and push it to stream of view
	return 0, nil
}
