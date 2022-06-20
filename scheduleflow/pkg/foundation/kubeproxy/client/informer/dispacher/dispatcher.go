package dispacher

import (
	"sync"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer/fundamental"
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

func New(eventMap fundamental.SubscribeEventMap) processor.ActorProcessorWithInitial {
	return &dispatcher{
		subscribeEventMap: eventMap,
		eventCache:        cmap.New[[]byte](),
	}
}

func (dis *dispatcher) Name() string {
	return "Dispatcher"
}

func (dis *dispatcher) Initial(ctx actor.Context) error {
	return nil
}

func (dis *dispatcher) CanProcess(msg interface{}) bool {
	switch msg.(type) {
	case *kubeproxy.CreateEvent, *kubeproxy.UpdateEvent, *kubeproxy.DeleteEvent:
		return true
	default:
		return false
	}
}

func (dis *dispatcher) Process(ctx actor.Context, env *actor.MessageEnvelope) (interface{}, error) {
	switch msg := env.Message.(type) {
	case *kubeproxy.CreateEvent:
		key, err := fundamental.FormKey(env.Sender, msg.GVR, kubeproxy.SubscribeAction_CREATE)
		if err != nil {
			return nil, err
		}
		go dis.onCreateEvent(key, msg, dis.subscribeEventMap)

	case *kubeproxy.UpdateEvent:
		key, err := fundamental.FormKey(env.Sender, msg.GVR, kubeproxy.SubscribeAction_UPDATE)
		if err != nil {
			return nil, err
		}
		go dis.onUpdateEvent(key, msg, dis.subscribeEventMap)

	case *kubeproxy.DeleteEvent:
		key, err := fundamental.FormKey(env.Sender, msg.GVR, kubeproxy.SubscribeAction_DELETE)
		if err != nil {
			return nil, err
		}
		go dis.onDeleteEvent(key, msg, dis.subscribeEventMap)

	}
	return nil, nil
}

func (dis *dispatcher) onCreateEvent(key string, msg *kubeproxy.CreateEvent, eventMap cmap.ConcurrentMap[cmap.ConcurrentMap[fundamental.Callbacker]]) {
	subscribers, ok := eventMap.Get(key)
	if !ok {
		logrus.Errorf("%s receive a create event not subscribed, detail: %s", logPrefix, msg.GVR.String())
		return
	}

	wg := sync.WaitGroup{}
	for name := range subscribers.Items() {
		wg.Add(1)
		go func(name string, event *kubeproxy.CreateEvent) {
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

func (dis *dispatcher) onDeleteEvent(key string, msg *kubeproxy.DeleteEvent, eventMap cmap.ConcurrentMap[cmap.ConcurrentMap[fundamental.Callbacker]]) {
	subscribers, ok := eventMap.Get(key)
	if !ok {
		logrus.Errorf("%s receive a create event not subscribed, detail: %s", logPrefix, msg.GVR.String())
		return
	}

	wg := sync.WaitGroup{}
	for name := range subscribers.Items() {
		wg.Add(1)
		go func(name string, event *kubeproxy.DeleteEvent) {
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

func (dis *dispatcher) onUpdateEvent(key string, msg *kubeproxy.UpdateEvent, eventMap cmap.ConcurrentMap[cmap.ConcurrentMap[fundamental.Callbacker]]) {
	subscribers, ok := eventMap.Get(key)
	if !ok {
		logrus.Errorf("%s receive a create event not subscribed, detail: %s", logPrefix, msg.GVR.String())
		return
	}

	wg := sync.WaitGroup{}
	for name := range subscribers.Items() {
		wg.Add(1)
		go func(name string, event *kubeproxy.UpdateEvent) {
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
