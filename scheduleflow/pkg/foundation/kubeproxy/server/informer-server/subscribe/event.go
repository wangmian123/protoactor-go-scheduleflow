package subscribe

import (
	"fmt"
	"sync"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

var json jsoniter.API

func init() {
	json = jsoniter.ConfigCompatibleWithStandardLibrary
}

// EventCreator creates events that subscribe resource information.
type EventCreator interface {
	CreateSubscribeEvent(subInfo *kubeproxy.SubscribeResource) cache.ResourceEventHandlerFuncs
	RenewSubscribeResource(info *kubeproxy.RenewSubscribeResource) (callbacks cache.ResourceEventHandlerFuncs, matched bool)
	UnsubscribeEvent(subInfo *kubeproxy.SubscribeResource) error
}

type eventCreator struct {
	tieContext      actor.Context
	subscriber      *actor.PID
	subscribeEvents map[string]*broker
}

func NewEventCreator(ctx actor.Context, sender *actor.PID) EventCreator {
	return &eventCreator{
		tieContext:      ctx,
		subscriber:      sender,
		subscribeEvents: make(map[string]*broker),
	}
}

// CreateSubscribeEvent creates recorder event which subscribes k8s resource.
func (ent *eventCreator) CreateSubscribeEvent(info *kubeproxy.SubscribeResource) cache.ResourceEventHandlerFuncs {
	if info.ActionCode <= 0 || info.ActionCode >= 1<<(kubeproxy.SubscribeAction_DELETE+1) {
		info.ActionCode = 1<<kubeproxy.SubscribeAction_CREATE | 1<<kubeproxy.SubscribeAction_UPDATE | 1<<kubeproxy.SubscribeAction_DELETE
	}
	subscribeEvent := broker{
		tieContext: ent.tieContext,
		subscriber: ent.subscriber,
		grv:        info.GVR.DeepCopy(),
		code:       kubeproxy.SubscribeAction(info.ActionCode),
	}

	if event, ok := ent.subscribeEvents[info.GVR.String()]; ok {
		event.close()
	}
	ent.subscribeEvents[info.GVR.String()] = &subscribeEvent

	var eventHandler cache.ResourceEventHandlerFuncs
	if info.ActionCode&(1<<kubeproxy.SubscribeAction_CREATE) != 0 {
		eventHandler.AddFunc = subscribeEvent.addFunc
	}
	if info.ActionCode&(1<<kubeproxy.SubscribeAction_UPDATE) != 0 {
		eventHandler.UpdateFunc = subscribeEvent.updateFunc
	}
	if info.ActionCode&(1<<kubeproxy.SubscribeAction_DELETE) != 0 {
		eventHandler.DeleteFunc = subscribeEvent.deleteFunc
	}

	return eventHandler
}

func (ent *eventCreator) RenewSubscribeResource(info *kubeproxy.RenewSubscribeResource) (callbacks cache.ResourceEventHandlerFuncs,
	matched bool) {
	event, ok := ent.subscribeEvents[info.GVR.String()]
	if ok {
		if info.ActionCode <= 0 || info.ActionCode >= 1<<(kubeproxy.SubscribeAction_DELETE+1) {
			info.ActionCode = 1<<kubeproxy.SubscribeAction_CREATE | 1<<kubeproxy.SubscribeAction_UPDATE | 1<<kubeproxy.SubscribeAction_DELETE
		}

		if event.code == kubeproxy.SubscribeAction(info.ActionCode) {
			return cache.ResourceEventHandlerFuncs{}, true
		}
	}

	return ent.CreateSubscribeEvent(&kubeproxy.SubscribeResource{
		GVR:        info.GVR,
		ActionCode: info.ActionCode,
		Option:     info.Option,
	}), false

}

func (ent *eventCreator) UnsubscribeEvent(subInfo *kubeproxy.SubscribeResource) error {
	if event, ok := ent.subscribeEvents[subInfo.GVR.String()]; ok {
		event.close()
		return nil
	}
	return fmt.Errorf("can not find recorder information %s", subInfo.GVR.String())
}

type broker struct {
	lock sync.RWMutex
	stop bool
	grv  *kubeproxy.GroupVersionResource
	code kubeproxy.SubscribeAction

	tieContext actor.Context
	subscriber *actor.PID
}

func (s *broker) close() {
	s.stop = true
}

func (s *broker) isStop() bool {
	stop := s.stop
	return stop
}

func (s *broker) addFunc(obj interface{}) {
	if s.isStop() {
		return
	}

	buffer, err := json.Marshal(obj)
	if err != nil {
		logrus.Errorf("%s marshal %T with error %v", logPrefix, obj, err)
		return
	}

	timestamp := v1.NewTime(time.Now())
	grv := s.grv.DeepCopy()
	s.tieContext.Request(s.subscriber, &kubeproxy.CreateEvent{
		Timestamp:   &timestamp,
		GVR:         grv,
		RawResource: buffer,
	})
}

func (s *broker) deleteFunc(obj interface{}) {
	if s.isStop() {
		return
	}

	unknown, ok := obj.(cache.DeletedFinalStateUnknown)
	if ok {
		obj = unknown.Obj
	}

	buffer, err := json.Marshal(obj)
	if err != nil {
		logrus.Errorf("%s marshal %T with error %v", logPrefix, obj, err)
		return
	}

	timestamp := v1.NewTime(time.Now())
	grv := s.grv.DeepCopy()
	s.tieContext.Request(s.subscriber, &kubeproxy.DeleteEvent{
		Timestamp:   &timestamp,
		GVR:         grv,
		RawResource: buffer,
	})
}

func (s *broker) updateFunc(oldResource, newResource interface{}) {
	if s.isStop() {
		return
	}

	buffer, err := json.Marshal(oldResource)
	if err != nil {
		logrus.Errorf("%s marshal %T with error %v", logPrefix, oldResource, err)
		return
	}

	bufferObj2, err := json.Marshal(newResource)
	if err != nil {
		logrus.Errorf("%s marshal %T with error %v", logPrefix, oldResource, err)
		return
	}

	timestamp := v1.NewTime(time.Now())
	s.tieContext.Request(s.subscriber, &kubeproxy.UpdateEvent{
		Timestamp:   &timestamp,
		GVR:         s.grv.DeepCopy(),
		OldResource: buffer,
		NewResource: bufferObj2,
	})
}
