package dispacher

import (
	"sync"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/k8sproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/k8sproxy/client/informer/fundamental"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
)

const (
	logPrefix = fundamental.LogPrefix + "[Dispatcher]"
)

type dispatcher struct {
	subscribeEventMap fundamental.SubscribeEventMap
	eventCache        cmap.ConcurrentMap[[]byte]
}

func New(eventMap fundamental.SubscribeEventMap) processor.ActorProcessor {
	return &dispatcher{
		subscribeEventMap: eventMap,
		eventCache:        cmap.New[[]byte](),
	}
}

func (dis *dispatcher) Name() string {
	return "Dispatcher"
}

func (dis *dispatcher) CanProcess(msg interface{}) bool {
	switch msg.(type) {
	case *k8sproxy.CreateEvent, *k8sproxy.UpdateEvent, *k8sproxy.DeleteEvent:
		return true
	default:
		return false
	}
}

func (dis *dispatcher) Process(ctx actor.Context) (interface{}, error) {
	switch msg := ctx.Message().(type) {
	case *k8sproxy.CreateEvent:
		key, err := fundamental.FormKey(ctx.Sender(), msg.GVR, k8sproxy.SubscribeAction_CREATE)
		if err != nil {
			return nil, err
		}
		go dis.onCreateEvent(key, msg, dis.subscribeEventMap)

	case *k8sproxy.UpdateEvent:
		key, err := fundamental.FormKey(ctx.Sender(), msg.GVR, k8sproxy.SubscribeAction_UPDATE)
		if err != nil {
			return nil, err
		}
		go dis.onUpdateEvent(key, msg, dis.subscribeEventMap)

	case *k8sproxy.DeleteEvent:
		key, err := fundamental.FormKey(ctx.Sender(), msg.GVR, k8sproxy.SubscribeAction_DELETE)
		if err != nil {
			return nil, err
		}
		go dis.onDeleteEvent(key, msg, dis.subscribeEventMap)

	}
	return nil, nil
}

func (dis *dispatcher) onCreateEvent(key string, msg *k8sproxy.CreateEvent, eventMap cmap.ConcurrentMap[cmap.ConcurrentMap[fundamental.Callbacker]]) {
	subscribers, ok := eventMap.Get(key)
	if !ok {
		logrus.Errorf("%s receive a create event not subscribed, detail: %s", logPrefix, msg.GVR.String())
		return
	}

	wg := sync.WaitGroup{}
	for name := range subscribers.Items() {
		wg.Add(1)
		go func(name string, event *k8sproxy.CreateEvent) {
			sub, ok := subscribers.Get(name)
			if !ok {
				logrus.Debugf("%s subscriber %s has been deleted", logPrefix, name)
				return
			}

			err := sub.ReceiveCreateEvent(*event)
			if err != nil {
				logrus.Errorf("%s dispacher message with error %v", logPrefix, err)
				return
			}
			defer wg.Done()
		}(name, msg.DeepCopy())
	}
	wg.Wait()
}

func (dis *dispatcher) onDeleteEvent(key string, msg *k8sproxy.DeleteEvent, eventMap cmap.ConcurrentMap[cmap.ConcurrentMap[fundamental.Callbacker]]) {
	subscribers, ok := eventMap.Get(key)
	if !ok {
		logrus.Errorf("%s receive a create event not subscribed, detail: %s", logPrefix, msg.GVR.String())
		return
	}

	wg := sync.WaitGroup{}
	for name := range subscribers.Items() {
		wg.Add(1)
		go func(name string, event *k8sproxy.DeleteEvent) {
			sub, ok := subscribers.Get(name)
			if !ok {
				logrus.Debugf("%s subscriber %s has been deleted", logPrefix, name)
				return
			}

			err := sub.ReceiveDeleteEvent(*event)
			if err != nil {
				logrus.Errorf("%s dispacher message with error %v", logPrefix, err)
				return
			}
			defer wg.Done()
		}(name, msg.DeepCopy())
	}
	wg.Wait()
}

func (dis *dispatcher) onUpdateEvent(key string, msg *k8sproxy.UpdateEvent, eventMap cmap.ConcurrentMap[cmap.ConcurrentMap[fundamental.Callbacker]]) {
	subscribers, ok := eventMap.Get(key)
	if !ok {
		logrus.Errorf("%s receive a create event not subscribed, detail: %s", logPrefix, msg.GVR.String())
		return
	}

	wg := sync.WaitGroup{}
	for name := range subscribers.Items() {
		wg.Add(1)
		go func(name string, event *k8sproxy.UpdateEvent) {
			sub, ok := subscribers.Get(name)
			if !ok {
				logrus.Debugf("%s subscriber %s has been deleted", logPrefix, name)
				return
			}

			err := sub.ReceiveUpdateEvent(*event)
			if err != nil {
				logrus.Errorf("%s dispacher message with error %v", logPrefix, err)
				return
			}
			defer wg.Done()
		}(name, msg.DeepCopy())
	}
	wg.Wait()
}
