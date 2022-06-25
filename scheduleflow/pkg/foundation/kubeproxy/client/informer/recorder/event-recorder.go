package recorder

import (
	"fmt"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer/fundamental"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	logPrefix        = fundamental.LogPrefix + "[Subscriber]"
	subscribeTimeout = 60 * time.Second
)

type subscribeEventRecorder struct {
	events fundamental.SubscribeEventMap
}

func New(events fundamental.SubscribeEventMap) processor.ActorProcessorWithInitial {
	return &subscribeEventRecorder{events: events}
}

func (sub *subscribeEventRecorder) Name() string {
	return "Subscriber"
}

func (sub *subscribeEventRecorder) Initial(_ actor.Context) error {
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
	default:
		return false
	}
}

func (sub *subscribeEventRecorder) Process(ctx actor.Context, env *actor.MessageEnvelope) (interface{}, error) {
	var err error
	if env.Sender == nil {
		return nil, fmt.Errorf("%s can not recorder event with no sender pid", logPrefix)
	}

	switch m := env.Message.(type) {
	case *fundamental.SubscribeResourceFrom[v1.Pod]:
		err = recordSubscribeEvent[v1.Pod](sub, ctx, env.Sender, m)
	case *fundamental.SubscribeResourceFrom[v1.Node]:
		err = recordSubscribeEvent[v1.Node](sub, ctx, env.Sender, m)
	case *fundamental.SubscribeResourceFrom[unstructured.Unstructured]:
		err = recordSubscribeEvent[unstructured.Unstructured](sub, ctx, env.Sender, m)
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

func recordSubscribeEvent[R any](recorder *subscribeEventRecorder, ctx actor.Context, sender *actor.PID,
	msg *fundamental.SubscribeResourceFrom[R]) error {
	logrus.Infof("%s recordes events %s from %s", logPrefix, msg.Resource.GVR.String(), msg.Source.String())
	subscribeEventKeys, err := fundamental.FormSubscriberKeys(msg.Source, msg.Resource.GVR, msg.Resource.ActionCode)
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
	resource kubeproxy.SubscribeResource) {
	// subscribe resource
	resource.ActionCode = 0
	subscribeFor := &kubeproxy.SubscribeResourceFor{
		Resource:   &resource,
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
		err = fmt.Errorf("%s subscrbe respond excepeted *kubeproxy.SubscribeConfirm,"+
			" but get %T", logPrefix, subRespond)
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

func recordSubscribeEvents[R any](msg *fundamental.SubscribeResourceFrom[R], sender *actor.PID,
	events fundamental.SubscribeEventMap) error {
	cb := fundamental.NewCallback[R](msg)
	subscribeEventKeys, err := fundamental.FormSubscriberKeys(msg.Source, msg.Resource.GVR, msg.Resource.ActionCode)
	if err != nil {
		return err
	}
	subscriberInfoKeys, err := fundamental.FormSubscriberKeys(sender, msg.Resource.GVR, msg.Resource.ActionCode)
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

		subsInfoMap := fundamental.SubscriberInformationMap{ConcurrentMap: cmap.New[fundamental.Callbacker]()}
		subsInfoMap.MSet(subsCallback)
		events.Set(sKey, &subsInfoMap)
	}

	return nil
}
