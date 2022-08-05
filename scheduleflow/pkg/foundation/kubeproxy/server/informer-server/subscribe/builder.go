package subscribe

import (
	"context"
	"fmt"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
)

const (
	logPrefix = "[InformerServer][InformerBuilder]"
)

type builder struct {
	dynamicFactory dynamicinformer.DynamicSharedInformerFactory

	tieContext actor.Context
	stopCtx    context.Context
	subscriber map[string]EventCreator
}

// NewInformerBuilder creates event creators which creates and records event
// that subscribes resource information from etcd, and informs subscribes.
func NewInformerBuilder(k8sConfig *rest.Config, syncInterval time.Duration, ctx context.Context) processor.ActorProcessorWithInitial {
	dyClient := dynamic.NewForConfigOrDie(k8sConfig)
	dyFactory := dynamicinformer.NewDynamicSharedInformerFactory(dyClient, syncInterval)
	return &builder{
		stopCtx:        ctx,
		dynamicFactory: dyFactory,
		subscriber:     make(map[string]EventCreator),
	}
}

// Name names processor.
func (b *builder) Name() string {
	return logPrefix
}

// CanProcess filters message processor can process.
func (b *builder) CanProcess(msg interface{}) bool {
	switch msg.(type) {
	case *kubeproxy.SubscribeResource, *kubeproxy.SubscribeResourceFor:
		return true
	case *kubeproxy.RenewSubscribeResource, *kubeproxy.RenewSubscribeResourceFor:
		return true
	case *kubeproxy.RenewSubscribeResourceGroupFor:
		return true
	default:
		return false
	}
}

// Process implements processor interface.
func (b *builder) Process(ctx actor.Context, env *actor.MessageEnvelope) (response interface{}, err error) {
	switch m := env.Message.(type) {
	case *kubeproxy.SubscribeResource:
		return b.subscribeResource(ctx, m, env.Sender)
	case *kubeproxy.SubscribeResourceFor:
		return b.subscribeResource(ctx, m.Resource, m.Subscriber)
	case *kubeproxy.RenewSubscribeResource:
		return b.renewSubscribeResource(ctx, m, env.Sender)
	case *kubeproxy.RenewSubscribeResourceFor:
		return b.renewSubscribeResource(ctx, m.Resource, m.Subscriber)
	case *kubeproxy.RenewSubscribeResourceGroupFor:
		return b.batchRenewSubscribeResource(ctx, m.Resources, m.Subscriber)
	default:
		return nil, nil
	}
}

// subscribeResource records subscriber pid, and create event creator,
// subscribing resource for subscriber.
func (b *builder) subscribeResource(ctx actor.Context, info *kubeproxy.SubscribeResource, subscriber *actor.PID) (*kubeproxy.SubscribeConfirm, error) {
	if subscriber == nil {
		return nil, fmt.Errorf("%s recorder fail, due to an empty subscriber", logPrefix)
	}
	logrus.Infof("%s subscriber %s subscribes type %s ", logPrefix, utils.FormActorKey(subscriber), kubeproxy.PrintGroupVersionResource(info.GVR))

	creator, ok := b.subscriber[subscriber.String()]
	if !ok {
		creator = NewEventCreator(ctx, subscriber)
		b.subscriber[subscriber.String()] = creator
	}
	event := creator.CreateSubscribeEvent(info)

	b.dynamicFactory.ForResource(kubeproxy.ConvertGVR(info.GVR)).Informer().AddEventHandler(event)
	b.dynamicFactory.Start(b.stopCtx.Done())
	return kubeproxy.NewSubscribeConfirm(info), nil
}

func (b *builder) batchRenewSubscribeResource(ctx actor.Context, resources []*kubeproxy.RenewSubscribeResource,
	subscriber *actor.PID) (*kubeproxy.SubscribeGroupConfirm, error) {
	if ctx == nil || resources == nil || subscriber == nil {
		return nil, fmt.Errorf("can not batch renew subcribing resource due to nil inputs")
	}

	batchResp := &kubeproxy.SubscribeGroupConfirm{Confirms: make([]*kubeproxy.SubscribeConfirm, 0, len(resources))}
	for _, res := range resources {
		resp, err := b.renewSubscribeResource(ctx, res, subscriber)
		if err != nil {
			return nil, err
		}
		batchResp.Confirms = append(batchResp.Confirms, resp)
	}

	return batchResp, nil
}

func (b *builder) renewSubscribeResource(ctx actor.Context, info *kubeproxy.RenewSubscribeResource, subscriber *actor.PID,
) (*kubeproxy.SubscribeConfirm, error) {
	if subscriber == nil {
		return nil, fmt.Errorf("%s recorder fail, due to an empty subscriber", logPrefix)
	}

	creator, ok := b.subscriber[subscriber.String()]
	if !ok {
		return b.subscribeResource(ctx, &kubeproxy.SubscribeResource{
			GVR:        info.GVR,
			ActionCode: info.ActionCode,
			Option:     info.Option,
		}, subscriber)
	}

	event, ok := creator.RenewSubscribeResource(info)
	if !ok {
		b.dynamicFactory.ForResource(kubeproxy.ConvertGVR(info.GVR)).Informer().AddEventHandler(event)
		b.dynamicFactory.Start(b.stopCtx.Done())
	}

	return kubeproxy.NewSubscribeConfirm(&kubeproxy.SubscribeResource{
		GVR:        info.GVR,
		ActionCode: info.ActionCode,
		Option:     info.Option,
	}), nil
}

// Initial start dynamic factory when actor start.
func (b *builder) Initial(ctx actor.Context) error {
	logrus.Infof("%s start informer factory", logPrefix)
	b.dynamicFactory.Start(b.stopCtx.Done())
	b.tieContext = ctx
	return nil
}
