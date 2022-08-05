package recorder

import (
	"context"
	"fmt"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer/fundamental"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
	"github.com/asynkron/protoactor-go/scheduler"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	logPrefix        = fundamental.LogPrefix + "[Subscriber]"
	subscribeTimeout = 60 * time.Second

	renewInterval = 30 * time.Second
)

type renewSubscribedResource struct {
}

type subscribeEventRecorder struct {
	goroutine context.Context
	events    fundamental.SubscribeEventMap
}

func New(events fundamental.SubscribeEventMap, rootCtx context.Context) processor.ActorProcessorWithInitial {
	return &subscribeEventRecorder{events: events, goroutine: rootCtx}
}

func (sub *subscribeEventRecorder) Name() string {
	return "Subscriber"
}

func (sub *subscribeEventRecorder) Initial(ctx actor.Context) error {
	logrus.Infof("%s starts renewing subscribed resource periodly, period: %s", logPrefix, renewInterval.String())
	cancel := scheduler.NewTimerScheduler(ctx).SendRepeatedly(renewInterval, renewInterval, ctx.Self(), &renewSubscribedResource{})
	go func() {
		for {
			select {
			case <-sub.goroutine.Done():
				cancel()
				return
			}
		}
	}()
	return nil
}

func (sub *subscribeEventRecorder) CanProcess(msg interface{}) bool {
	switch msg.(type) {
	case *fundamental.SubscribeResourceFrom[v1.Pod]:
		return true
	case *fundamental.SubscribeResourceFrom[v1.Node]:
		return true
	case *fundamental.SubscribeResourceFrom[unstructured.Unstructured]:
		return true
	case *renewSubscribedResource:
		return true
	default:
		return false
	}
}

func (sub *subscribeEventRecorder) Process(ctx actor.Context, env *actor.MessageEnvelope) (interface{}, error) {
	switch env.Message.(type) {
	case *fundamental.SubscribeResourceFrom[v1.Pod], *fundamental.SubscribeResourceFrom[v1.Node],
		*fundamental.SubscribeResourceFrom[unstructured.Unstructured]:
		return sub.receiveSubscribeEvents(ctx, env)
	default:
		envelop := &actor.MessageEnvelope{
			Message: env.Message,
			Sender:  env.Sender,
		}
		go func(envelop *actor.MessageEnvelope) {
			resp, err := sub.asyncProcess(ctx, env)
			if err != nil {
				logrus.Errorf("%s processes with error %v", logPrefix, err)
				return
			}

			if resp != nil {
				ctx.Send(envelop.Sender, resp)
			}
		}(envelop)
		return nil, nil
	}
}

func (sub *subscribeEventRecorder) asyncProcess(ctx actor.Context, env *actor.MessageEnvelope) (interface{}, error) {
	switch m := env.Message.(type) {
	case *renewSubscribedResource:
		return nil, sub.renewSubscribedResource(ctx)
	default:
		return nil, fmt.Errorf("%s can not process message type %T", logPrefix, m)
	}
}

func (sub *subscribeEventRecorder) receiveSubscribeEvents(ctx actor.Context, env *actor.MessageEnvelope) (*fundamental.SubscribeRespond, error) {
	var err error
	if env.Sender == nil {
		return nil, fmt.Errorf("%s can not recorder event with no sender pid", logPrefix)
	}

	switch m := env.Message.(type) {
	case *fundamental.SubscribeResourceFrom[v1.Pod]:
		err = receiveSubscribeEvents[v1.Pod](sub, ctx, env.Sender, m)
	case *fundamental.SubscribeResourceFrom[v1.Node]:
		err = receiveSubscribeEvents[v1.Node](sub, ctx, env.Sender, m)
	case *fundamental.SubscribeResourceFrom[unstructured.Unstructured]:
		err = receiveSubscribeEvents[unstructured.Unstructured](sub, ctx, env.Sender, m)
	}

	if err != nil {
		logrus.Errorf("%s %v", logPrefix, err)
		return &fundamental.SubscribeRespond{
			Code:    fundamental.Fail,
			Message: err,
		}, nil
	}

	return &fundamental.SubscribeRespond{
		Code:    fundamental.Success,
		Message: nil,
	}, nil
}

func receiveSubscribeEvents[R any](recorder *subscribeEventRecorder, ctx actor.Context, sender *actor.PID,
	msg *fundamental.SubscribeResourceFrom[R]) error {
	logrus.Infof("%s recordes events %s from %s", logPrefix, msg.Resource.GVR.String(), msg.Source.String())
	subscribeEventKeys, err := fundamental.FormSubscribeEventKeys(msg.Source, msg.Resource.GVR, msg.Resource.ActionCode)
	if err != nil {
		return err
	}

	var resourceSubscribed bool
	for _, key := range subscribeEventKeys {
		_, ok := recorder.events.Get(key)
		if ok {
			resourceSubscribed = true
		}
	}

	err = recordSubscribeEvents[R](msg, sender, recorder.events)
	if err != nil {
		return err
	}

	if resourceSubscribed {
		return nil
	}

	go recorder.subscribeResource(ctx, msg.Source, sender, msg.Resource)
	return nil
}

func (sub *subscribeEventRecorder) subscribeResource(ctx actor.Context, sourcePID, respondTo *actor.PID,
	resource *kubeproxy.SubscribeResource) {
	// subscribe resource with all action type for cache resource.
	resource.ActionCode = 0
	subscribeFor := &kubeproxy.SubscribeResourceFor{
		Resource:   resource,
		Subscriber: ctx.Self(),
	}
	future := ctx.RequestFuture(sourcePID, subscribeFor, subscribeTimeout)
	result, err := future.Result()
	if err != nil {
		logrus.Errorf("%s subscribe error %v", logPrefix, err)
		ctx.Send(respondTo, &fundamental.SubscribeRespond{
			Code:    fundamental.Fail,
			Message: err,
		})
		return
	}

	subRespond, ok := result.(*kubeproxy.SubscribeConfirm)
	if !ok {
		err = fmt.Errorf("%s subscrbe respond excepeted *kubeproxy.SubscribeConfirm, but get %T", logPrefix, subRespond)
		ctx.Send(respondTo, &fundamental.SubscribeRespond{
			Code:    fundamental.Fail,
			Message: err,
		})
		logrus.Error(err)
		return
	}

	ctx.Send(respondTo, &fundamental.SubscribeRespond{
		Code:    fundamental.Success,
		Message: subRespond,
	})
	logrus.Infof("%s recorder resource %s for %s from %s success", logPrefix, resource.String(),
		respondTo.String(), sourcePID.String())
}

func (sub *subscribeEventRecorder) renewSubscribedResource(ctx actor.Context) error {
	subscribeResources := make(map[string]map[string]struct{}, len(sub.events.Items()))
	stringToPID := make(map[string]*actor.PID)
	stringToGVR := make(map[string]*kubeproxy.GroupVersionResource)
	for key := range sub.events.Items() {
		eventInfo, err := fundamental.FormEvenKeyStruct(key)
		if err != nil {
			return err
		}

		stringToPID[eventInfo.PID.String()] = eventInfo.PID
		gvrMap, ok := subscribeResources[eventInfo.PID.String()]
		if !ok {
			gvrMap = make(map[string]struct{})
			subscribeResources[eventInfo.PID.String()] = gvrMap
		}
		gvrMap[eventInfo.GroupVersionResource.String()] = struct{}{}
		stringToGVR[eventInfo.GroupVersionResource.String()] = eventInfo.GroupVersionResource
	}

	if len(subscribeResources) == 0 {
		return nil
	}

	for pidStr, gvrMap := range subscribeResources {
		pid, ok := stringToPID[pidStr]
		if !ok {
			return fmt.Errorf("can not find pid in stringToPID")
		}
		if len(gvrMap) == 0 {
			continue
		}
		renewResource := &kubeproxy.RenewSubscribeResourceGroupFor{
			Resources:  make([]*kubeproxy.RenewSubscribeResource, 0, len(gvrMap)),
			Subscriber: ctx.Self(),
		}

		for gvrStr := range gvrMap {
			gvr, ok := stringToGVR[gvrStr]
			if !ok {
				return fmt.Errorf("can not find gvr in stringToGVR")
			}
			renewResource.Resources = append(renewResource.Resources, &kubeproxy.RenewSubscribeResource{
				GVR:        gvr,
				ActionCode: 0,
			})
		}

		err := sub.renewResourceFrom(ctx, pid, renewResource)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sub *subscribeEventRecorder) renewResourceFrom(ctx actor.Context, sourcePID *actor.PID,
	renew *kubeproxy.RenewSubscribeResourceGroupFor) error {
	result, err := ctx.RequestFuture(sourcePID, renew, subscribeTimeout).Result()
	if err != nil {
		return err
	}
	resp, ok := result.(*kubeproxy.SubscribeGroupConfirm)
	if !ok {
		return fmt.Errorf("receive respond type error except *kubeproxy.SubscribeGroupConfirm, but get %T", result)
	}
	if len(resp.Confirms) != len(renew.Resources) {
		return fmt.Errorf("confirm length is not equal to subscribed resource")
	}
	return nil
}

func recordSubscribeEvents[R any](msg *fundamental.SubscribeResourceFrom[R], sender *actor.PID,
	events fundamental.SubscribeEventMap) error {
	cb := fundamental.NewCallback[R](msg)
	eventKeys, err := fundamental.FormSubscribeEventKeys(msg.Source, msg.Resource.GVR, msg.Resource.ActionCode)
	if err != nil {
		return err
	}

	subscriberKey, err := fundamental.FormPIDGVRKeyString(sender, msg.Resource.GVR)
	if err != nil {
		return err
	}
	for _, eventKey := range eventKeys {
		subscribeInfo, ok := events.Get(eventKey)
		if ok {
			subscribeInfo.BatchSet(subscriberKey, cb)
			continue
		}
		subscribeInfo = &fundamental.SubscriberInformationMap{ConcurrentMap: cmap.New[[]fundamental.Callbacker]()}
		subscribeInfo.BatchSet(subscriberKey, cb)
		events.Set(eventKey, subscribeInfo)
		logrus.Infof("%s event key %s has add", logPrefix, eventKey)
	}
	return nil
}
