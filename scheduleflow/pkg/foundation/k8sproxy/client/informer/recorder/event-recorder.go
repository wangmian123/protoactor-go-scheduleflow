package recorder

import (
	"fmt"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/k8sproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/k8sproxy/client/informer/fundamental"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	logPrefix = fundamental.LogPrefix + "[Subscriber]"

	subscribeTimeout = 60 * time.Second
)

type subscribeEventRecorder struct {
	events fundamental.SubscribeEventMap
}

func New(events fundamental.SubscribeEventMap) processor.ActorProcessor {
	return &subscribeEventRecorder{events: events}
}

func (sub *subscribeEventRecorder) Name() string {
	return "Subscriber"
}

func (sub *subscribeEventRecorder) CanProcess(msg interface{}) bool {
	switch msg.(type) {
	case *fundamental.SubscribeResourceFrom[v1.Pod]:
		return true
	case *fundamental.SubscribeResourceFrom[v1.Node]:
		return true
	case *fundamental.SubscribeResourceFrom[unstructured.Unstructured]:
		return true
	default:
		return false
	}
}

func (sub *subscribeEventRecorder) Process(ctx actor.Context) (interface{}, error) {
	var err error
	if ctx.Sender() == nil {
		return nil, fmt.Errorf("can not recorder event with no sender pid")
	}

	switch msg := ctx.Message().(type) {
	case *fundamental.SubscribeResourceFrom[v1.Pod]:
		err = recordSubscribeEvent[v1.Pod](sub, ctx, msg)
	case *fundamental.SubscribeResourceFrom[v1.Node]:
		err = recordSubscribeEvent[v1.Node](sub, ctx, msg)
	case *fundamental.SubscribeResourceFrom[unstructured.Unstructured]:
		err = recordSubscribeEvent[unstructured.Unstructured](sub, ctx, msg)
	}

	if err != nil {
		logrus.Errorf("%s %v", logPrefix, err)
		return fundamental.SubscribeRespond{
			Code:    fundamental.Fail,
			Message: err,
		}, nil
	}

	return nil, nil
}

func recordSubscribeEvent[R any](recorder *subscribeEventRecorder, ctx actor.Context, msg *fundamental.SubscribeResourceFrom[R]) error {
	logrus.Infof("%s receive recorder %s from %s", logPrefix, msg.Resource.String(), msg.Target.String())
	err := recordSubscribeEvents[R](msg, ctx.Sender(), recorder.events)
	if err != nil {
		return err
	}

	go recorder.subscribeResource(ctx, msg.Target, ctx.Sender(), msg.Resource)
	return nil
}

func (sub *subscribeEventRecorder) subscribeResource(ctx actor.Context, target, respondTo *actor.PID, resource k8sproxy.SubscribeResource) {
	future := ctx.RequestFuture(target, &resource, subscribeTimeout)
	result, err := future.Result()
	if err != nil {
		logrus.Errorf("%s subscrbe error %v", logPrefix, err)
		ctx.Send(respondTo, &fundamental.SubscribeRespond{
			Code:    fundamental.Fail,
			Message: err,
		})
		return
	}

	subRespond, ok := result.(*k8sproxy.SubscribeConfirm)
	if !ok {
		err = fmt.Errorf("%s subscrbe respond excepeted *k8sproxy.SubscribeConfirm, but get %T", logPrefix, subRespond)
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
	logrus.Infof("%s recorder resource %s for %s from %s success", logPrefix, resource.String(), respondTo.String(), target.String())
}

func recordSubscribeEvents[R any](msg *fundamental.SubscribeResourceFrom[R], sender *actor.PID,
	events cmap.ConcurrentMap[cmap.ConcurrentMap[fundamental.Callbacker]]) error {
	cb := fundamental.NewCallback[R](msg)
	subscribeEventKeys, err := formSubscriberKeys(msg.Target, msg.Resource.GVR, msg.Resource.ActionCode)
	if err != nil {
		return err
	}
	subscriberInfoKeys, err := formSubscriberKeys(sender, msg.Resource.GVR, msg.Resource.ActionCode)
	if err != nil {
		return err
	}

	subsCallback := make(map[string]fundamental.Callbacker, len(subscribeEventKeys))
	for _, key := range subscriberInfoKeys {
		subsCallback[key] = cb
	}

	for _, sKey := range subscribeEventKeys {
		sInfo, ok := events.Get(sKey)
		if ok {
			sInfo.MSet(subsCallback)
			continue
		}

		subsInfoMap := cmap.New[fundamental.Callbacker]()
		subsInfoMap.MSet(subsCallback)
		events.Set(sKey, subsInfoMap)
	}

	return nil
}

func formSubscriberKeys(target *actor.PID, gvr *k8sproxy.GroupVersionResource, code int32) ([]string, error) {
	actionTypes := k8sproxy.GenerateSubscribeAction(code)

	keys := make([]string, 0, len(actionTypes))
	for _, actType := range actionTypes {
		key, err := fundamental.FormKey(target, gvr, actType)
		if err != nil {
			logrus.Errorf("%s from key error %v", logPrefix, err)
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}
