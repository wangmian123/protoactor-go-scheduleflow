package fundamental

import (
	"fmt"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	cmap "github.com/orcaman/concurrent-map"
)

// SubscriberInformationMap records recorder pid, gvr and action type
// map[`subscriber.PID + subscribe.GVR`] Callbacker list
type SubscriberInformationMap struct {
	cmap.ConcurrentMap[[]Callbacker]
}

func (sub *SubscriberInformationMap) BatchSet(key string, callers ...Callbacker) {
	if len(callers) == 0 {
		return
	}

	stored, ok := sub.Get(key)
	if !ok {
		sub.Set(key, callers)
		return
	}
	stored = append(stored, callers...)
	sub.Set(key, stored)
}

// SubscribeEventMap records target pid where subscribed event from.
// map[`resourceSource.PID + subscribe.GVR + subscribe.SubscribeAction`]*SubscriberInformationMap
type SubscribeEventMap struct {
	cmap.ConcurrentMap[*SubscriberInformationMap]
}

// Callbacker records subscribers' handlers and callbacks when
// receives relative resource changing actions.
type Callbacker interface {
	ReceiveCreateEvent(event *kubeproxy.CreateEvent) error
	ReceiveUpdateEvent(event *kubeproxy.UpdateEvent) error
	ReceiveDeleteEvent(event *kubeproxy.DeleteEvent) error
}

type callback[R any] struct {
	gvr     *kubeproxy.GroupVersionResource
	handler ResourceEventHandlerFuncs[R]
}

func (s *callback[R]) ReceiveCreateEvent(event *kubeproxy.CreateEvent) error {
	if event.GetGVR().String() != s.gvr.String() {
		return fmt.Errorf("receive invalid gvr type")
	}

	res, err := s.unmarshal(event.RawResource)
	if err != nil {
		return err
	}
	go s.handler.AddFunc(res)
	return nil
}

func (s *callback[R]) ReceiveUpdateEvent(event *kubeproxy.UpdateEvent) error {
	if event.GetGVR().String() != s.gvr.String() {
		return fmt.Errorf("receive invalid gvr type")
	}

	oldOne, err := s.unmarshal(event.OldResource)
	if err != nil {
		return err
	}
	newOne, err := s.unmarshal(event.NewResource)
	if err != nil {
		return err
	}
	go s.handler.UpdateFunc(oldOne, newOne)
	return nil
}

func (s *callback[R]) ReceiveDeleteEvent(event *kubeproxy.DeleteEvent) error {
	if event.GetGVR().String() != s.gvr.String() {
		return fmt.Errorf("receive invalid gvr type")
	}

	res, err := s.unmarshal(event.RawResource)
	if err != nil {
		return err
	}
	go s.handler.DeleteFunc(res)
	return nil
}

func (s *callback[R]) unmarshal(data []byte) (R, error) {
	if s.handler != nil {
		return s.handler.Unmarshal(data)
	}
	var res R
	err := json.Unmarshal(data, &res)
	if err != nil {
		return res, fmt.Errorf("unmarshal gvr error with %v", err)
	}
	return res, nil
}

func NewCallback[R any](info *SubscribeResourceFrom[R]) *callback[R] {
	return &callback[R]{
		gvr:     info.Resource.GVR,
		handler: info.Handler,
	}
}
