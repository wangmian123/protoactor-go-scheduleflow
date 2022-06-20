package subscribe

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

type EventCreator interface {
	CreateSubscribeEvent(subInfo *kubeproxy.SubscribeResource) cache.ResourceEventHandlerFuncs
	UnsubscribeEvent(subInfo *kubeproxy.SubscribeResource) error
}

type eventCreator struct {
	tieContext actor.Context
	subscriber *actor.PID

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
func (ent *eventCreator) CreateSubscribeEvent(subInfo *kubeproxy.SubscribeResource) cache.ResourceEventHandlerFuncs {
	subscribeEvent := broker{
		tieContext: ent.tieContext,
		subscriber: ent.subscriber,
		grv:        *subInfo.GVR,
	}

	if event, ok := ent.subscribeEvents[subInfo.GVR.String()]; ok {
		event.close()
	}

	ent.subscribeEvents[subInfo.GVR.String()] = &subscribeEvent

	var eventHandler cache.ResourceEventHandlerFuncs

	if subInfo.ActionCode <= 0 || subInfo.ActionCode >= 1<<(kubeproxy.SubscribeAction_DELETE+1) {
		subInfo.ActionCode = 1<<kubeproxy.SubscribeAction_CREATE | 1<<kubeproxy.SubscribeAction_UPDATE | 1<<kubeproxy.SubscribeAction_DELETE
	}

	if subInfo.ActionCode&1<<kubeproxy.SubscribeAction_CREATE != 0 {
		eventHandler.AddFunc = subscribeEvent.addFunc
	}
	if subInfo.ActionCode&1<<kubeproxy.SubscribeAction_UPDATE != 0 {
		eventHandler.UpdateFunc = subscribeEvent.updateFunc
	}
	if subInfo.ActionCode&1<<kubeproxy.SubscribeAction_DELETE != 0 {
		eventHandler.DeleteFunc = subscribeEvent.deleteFunc
	}

	return eventHandler
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
	grv  kubeproxy.GroupVersionResource

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
	grv := s.grv
	s.tieContext.Request(s.subscriber, &kubeproxy.CreateEvent{
		Timestamp:   &timestamp,
		GVR:         &grv,
		RawResource: buffer,
	})
}

func (s *broker) deleteFunc(obj interface{}) {
	if s.isStop() {
		return
	}

	buffer, err := json.Marshal(obj)
	if err != nil {
		logrus.Errorf("%s marshal %T with error %v", logPrefix, obj, err)
		return
	}

	timestamp := v1.NewTime(time.Now())
	grv := s.grv
	s.tieContext.Request(s.subscriber, &kubeproxy.DeleteEvent{
		Timestamp:   &timestamp,
		GVR:         &grv,
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
	grv := s.grv
	s.tieContext.Request(s.subscriber, &kubeproxy.UpdateEvent{
		Timestamp:   &timestamp,
		GVR:         &grv,
		OldResource: buffer,
		NewResource: bufferObj2,
	})
}
