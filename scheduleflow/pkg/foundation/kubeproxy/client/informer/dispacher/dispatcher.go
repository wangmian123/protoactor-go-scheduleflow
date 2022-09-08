package dispacher

import (
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer/fundamental"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
	jsoniter "github.com/json-iterator/go"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var json jsoniter.API

func init() {
	json = jsoniter.ConfigCompatibleWithStandardLibrary
}

type delayedFlushCreateEvent struct {
	sourcePID  *actor.PID
	subscriber *actor.PID
	resource   *kubeproxy.SubscribeResource
}

const (
	logPrefix = fundamental.LogPrefix + "[Dispatcher]"
)

type dispatcher struct {
	subscribeEventMap fundamental.SubscribeEventMap
	eventCache        cmap.ConcurrentMap[cmap.ConcurrentMap[[]byte]]
}

func New(eventMap fundamental.SubscribeEventMap) processor.ActorProcessorWithInitial {
	return &dispatcher{
		subscribeEventMap: eventMap,
		eventCache:        cmap.New[cmap.ConcurrentMap[[]byte]](),
	}
}

func (dis *dispatcher) Name() string {
	return "Dispatcher"
}

func (dis *dispatcher) Initial(_ actor.Context) error {
	return nil
}

func (dis *dispatcher) CanProcess(msg interface{}) bool {
	switch msg.(type) {
	case *kubeproxy.CreateEvent, *kubeproxy.UpdateEvent, *kubeproxy.DeleteEvent:
		return true
	case fundamental.SubscribeSourceInformation:
		return true
	case *delayedFlushCreateEvent:
		return true
	default:
		return false
	}
}

func (dis *dispatcher) Process(ctx actor.Context, env *actor.MessageEnvelope) (interface{}, error) {
	switch msg := env.Message.(type) {
	case *kubeproxy.CreateEvent:
		go dis.onCreateEvent(env.Sender, msg)

	case *kubeproxy.UpdateEvent:
		go dis.onUpdateEvent(env.Sender, msg)

	case *kubeproxy.DeleteEvent:
		go dis.onDeleteEvent(env.Sender, msg)

	case fundamental.SubscribeSourceInformation:
		ctx.Send(ctx.Self(), &delayedFlushCreateEvent{sourcePID: msg.SourcePID(), subscriber: env.Sender,
			resource: msg.SubscribeResource()})

	case *delayedFlushCreateEvent:
		go dis.flushCreateEvents(msg)
	}
	return nil, nil
}

func (dis *dispatcher) onCreateEvent(sourcePID *actor.PID, msg *kubeproxy.CreateEvent) {
	subscribers, err := dis.findEventSubscribers(sourcePID, msg.GVR, kubeproxy.SubscribeAction_CREATE)
	if err != nil {
		//logrus.Warning(err)
		return
	}

	dis.cacheCreateEvent(sourcePID, msg)
	dis.broadcastCreateEvent(msg, subscribers)
}

func (dis *dispatcher) cacheCreateEvent(sourcePID *actor.PID, msg *kubeproxy.CreateEvent) {
	cache := dis.getOrCreateCache(sourcePID, msg.GVR)

	metaKey, err := dis.getMetadataKey(msg.RawResource)
	if err != nil {
		logrus.Errorf("%s can not cache create event due to %v", logPrefix, err)
	}
	cache.Set(metaKey, msg.RawResource)
}

func (dis *dispatcher) broadcastCreateEvent(msg *kubeproxy.CreateEvent, subscribers *fundamental.SubscriberInformationMap) {
	wg := sync.WaitGroup{}
	for name := range subscribers.Items() {
		wg.Add(1)
		go func(name string, event *kubeproxy.CreateEvent) {
			subs, ok := subscribers.Get(name)
			if !ok {
				logrus.Debugf("%s subscriber %s has been deleted", logPrefix, name)
				return
			}

			for _, sub := range subs {
				err := sub.ReceiveCreateEvent(event)
				if err != nil {
					logrus.Errorf("%s dispacher message with error %v", logPrefix, err)
					return
				}
			}

			defer wg.Done()
		}(name, msg.DeepCopy())
	}
	wg.Wait()
}

func (dis *dispatcher) flushCreateEvents(info *delayedFlushCreateEvent) {
	key := formPidGvrKey(info.sourcePID, info.resource.GVR)
	cache, ok := dis.eventCache.Get(key)
	if !ok {
		logrus.Warningf("%s can not get event cache", logPrefix)
		return
	}

	subscribers, err := dis.findEventSubscribers(info.sourcePID, info.resource.GVR, kubeproxy.SubscribeAction_CREATE)
	if err != nil {
		//logrus.Warning(err)
		return
	}

	key, err = fundamental.FormPIDGVRKeyString(info.subscriber, info.resource.GVR)
	if err != nil {
		logrus.Errorf("can not flushCreateEvents due to %v", err)
		return
	}

	callbacks, ok := subscribers.Get(key)
	if !ok {
		logrus.Errorf("%s can not get callback %s", logPrefix, key)
		return
	}

	now := metav1.Now()
	for _, res := range cache.Items() {
		for _, callback := range callbacks {
			err = callback.ReceiveCreateEvent(&kubeproxy.CreateEvent{Timestamp: &now, GVR: info.resource.GVR, RawResource: res})
			if err != nil {
				logrus.Errorf("%s dispacher message with error %v", logPrefix, err)
			}
		}
	}
}

func (dis *dispatcher) onDeleteEvent(sourcePID *actor.PID, msg *kubeproxy.DeleteEvent) {
	subscribers, err := dis.findEventSubscribers(sourcePID, msg.GVR, kubeproxy.SubscribeAction_DELETE)
	if err != nil {
		//logrus.Warning(err)
		return
	}

	dis.cacheDeleteEvent(sourcePID, msg)
	dis.broadcastDeleteEvent(msg, subscribers)
}

func (dis *dispatcher) cacheDeleteEvent(sourcePID *actor.PID, msg *kubeproxy.DeleteEvent) {
	key := formPidGvrKey(sourcePID, msg.GVR)
	cache, ok := dis.eventCache.Get(key)
	if !ok {
		logrus.Errorf("%s receive delete event without cache store", logPrefix)
		return
	}

	metaKey, err := dis.getMetadataKey(msg.RawResource)
	if err != nil {
		logrus.Error(err)
		return
	}
	cache.Remove(metaKey)
}

func (dis *dispatcher) broadcastDeleteEvent(msg *kubeproxy.DeleteEvent, subscribers *fundamental.SubscriberInformationMap) {
	wg := sync.WaitGroup{}
	for name := range subscribers.Items() {
		wg.Add(1)
		go func(name string, event *kubeproxy.DeleteEvent) {
			subs, ok := subscribers.Get(name)
			if !ok {
				logrus.Debugf("%s subscriber %s has been deleted", logPrefix, name)
				return
			}

			for _, sub := range subs {
				err := sub.ReceiveDeleteEvent(event)
				if err != nil {
					logrus.Errorf("%s dispacher message with error %v", logPrefix, err)
					return
				}
			}

			defer wg.Done()
		}(name, msg.DeepCopy())
	}
	wg.Wait()
}

func (dis *dispatcher) onUpdateEvent(sourcePID *actor.PID, msg *kubeproxy.UpdateEvent) {
	subscribers, err := dis.findEventSubscribers(sourcePID, msg.GVR, kubeproxy.SubscribeAction_UPDATE)
	if err != nil {
		//logrus.Warning(err)
		return
	}
	dis.cacheUpdateEvent(sourcePID, msg)
	dis.broadcastUpdateEvent(msg, subscribers)
}

func (dis *dispatcher) cacheUpdateEvent(sourcePID *actor.PID, msg *kubeproxy.UpdateEvent) {
	key := formPidGvrKey(sourcePID, msg.GVR)
	cache, ok := dis.eventCache.Get(key)
	if !ok {
		dis.cacheCreateEvent(sourcePID, &kubeproxy.CreateEvent{
			GVR:         msg.GVR,
			RawResource: msg.NewResource,
		})
		return
	}

	metaKey, err := dis.getMetadataKey(msg.NewResource)
	if err != nil {
		logrus.Error(err)
	}
	cache.Set(metaKey, msg.NewResource)
}

func (dis *dispatcher) broadcastUpdateEvent(msg *kubeproxy.UpdateEvent, subscribers *fundamental.SubscriberInformationMap) {
	wg := sync.WaitGroup{}
	for name := range subscribers.Items() {
		wg.Add(1)
		go func(name string, event *kubeproxy.UpdateEvent) {
			subs, ok := subscribers.Get(name)
			if !ok {
				return
			}
			for _, sub := range subs {
				err := sub.ReceiveUpdateEvent(event)
				if err != nil {
					logrus.Errorf("%s dispacher message with error %v", logPrefix, err)
					return
				}
			}
			defer wg.Done()
		}(name, msg.DeepCopy())
	}
	wg.Wait()
}

func (dis *dispatcher) findEventSubscribers(sourcePID *actor.PID, gvr *kubeproxy.GroupVersionResource,
	act kubeproxy.SubscribeAction) (*fundamental.SubscriberInformationMap, error) {
	key, err := fundamental.FormEventKeyString(sourcePID, gvr, act)
	if err != nil {
		return nil, fmt.Errorf("%s form key error %v", logPrefix, err)
	}

	subscribers, ok := dis.subscribeEventMap.Get(key)
	if !ok {
		return nil, fmt.Errorf("%s receive a create event not subscribed, detail: %s", logPrefix, key)
	}

	return subscribers, nil
}

func (dis *dispatcher) getOrCreateCache(sourcePID *actor.PID, gvr *kubeproxy.GroupVersionResource) cmap.ConcurrentMap[[]byte] {
	key := formPidGvrKey(sourcePID, gvr)
	cache, ok := dis.eventCache.Get(key)
	if !ok {
		cache = cmap.New[[]byte]()
		dis.eventCache.Set(key, cache)
	}
	return cache
}

func (dis *dispatcher) getMetadataKey(raw []byte) (string, error) {
	var obj unstructured.Unstructured
	err := json.Unmarshal(raw, &obj)
	if err != nil {
		return "", err
	}

	name := getNestedString(obj.Object, "metadata", "name")
	namespace := getNestedString(obj.Object, "metadata", "namespace")
	if namespace == "" {
		return name, nil
	}
	return namespace + "/" + name, nil
}

func formPidGvrKey(pid *actor.PID, gvr *kubeproxy.GroupVersionResource) string {
	pidName := fmt.Sprintf("%s/%s", pid.Address, pid.Id)
	gvrName := fmt.Sprintf("%s.%s.%s", gvr.Group, gvr.Version, gvr.Resource)
	return fmt.Sprintf("%s-%s", pidName, gvrName)
}

func getNestedString(obj map[string]interface{}, fields ...string) string {
	val, found, err := unstructured.NestedString(obj, fields...)
	if !found || err != nil {
		return ""
	}
	return val
}
