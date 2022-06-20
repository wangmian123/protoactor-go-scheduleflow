package fundamental

import (
	"encoding/json"
	"fmt"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	cmap "github.com/orcaman/concurrent-map"
)

// SubscriberInformationMap records recorder pid, gvr and action type
// map[`recorder.PID + SubscribeEventMap.GVR + SubscribeEventMap.SubscribeAction`]callback
type SubscriberInformationMap = cmap.ConcurrentMap[Callbacker]

// SubscribeEventMap records target pid where subscribed event from.
// map[`recorder.PID + SubscribeEventMap.GVR + SubscribeEventMap.SubscribeAction`]
type SubscribeEventMap = cmap.ConcurrentMap[SubscriberInformationMap]

type Callbacker interface {
	ReceiveCreateEvent(event kubeproxy.CreateEvent) error
	ReceiveUpdateEvent(event kubeproxy.UpdateEvent) error
	ReceiveDeleteEvent(event kubeproxy.DeleteEvent) error
}

type callback[R any] struct {
	gvr     kubeproxy.GroupVersionResource
	handler ResourceEventHandlerFuncs[R]
}

func (s *callback[R]) ReceiveCreateEvent(event kubeproxy.CreateEvent) error {
	if event.GetGVR().String() != s.gvr.String() {
		return fmt.Errorf("receive invalid gvr type")
	}

	res, err := s.unmarshal(event.RawResource)
	if err != nil {
		return err
	}
	s.handler.AddFunc(res)
	return nil
}

func (s *callback[R]) ReceiveUpdateEvent(event kubeproxy.UpdateEvent) error {
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
	s.handler.UpdateFunc(oldOne, newOne)
	return nil
}

func (s *callback[R]) ReceiveDeleteEvent(event kubeproxy.DeleteEvent) error {
	if event.GetGVR().String() != s.gvr.String() {
		return fmt.Errorf("receive invalid gvr type")
	}

	res, err := s.unmarshal(event.RawResource)
	if err != nil {
		return err
	}
	s.handler.DeleteFunc(res)
	return nil
}

func (s *callback[R]) unmarshal(data []byte) (R, error) {
	if s.handler.Unmarshal != nil {
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
		gvr:     *info.Resource.GVR,
		handler: info.Handler,
	}
}
