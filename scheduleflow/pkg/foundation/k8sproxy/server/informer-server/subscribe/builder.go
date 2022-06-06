package subscribe

import (
	"context"
	"fmt"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/k8sproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const logPrefix = "[InformerBuilder]"

type builder struct {
	informerFactory informers.SharedInformerFactory
	dynamicFactory  dynamicinformer.DynamicSharedInformerFactory

	tieContext actor.Context
	stopCtx    context.Context
	subscriber map[string]EventCreator
}

func NewInformerBuilder(k8sConfig *rest.Config, syncInterval time.Duration, ctx context.Context) processor.ActorProcessorWithInitial {
	clientSet := kubernetes.NewForConfigOrDie(k8sConfig)
	inFactory := informers.NewSharedInformerFactory(clientSet, syncInterval)

	dyClient := dynamic.NewForConfigOrDie(k8sConfig)
	dyFactory := dynamicinformer.NewDynamicSharedInformerFactory(dyClient, syncInterval)
	return &builder{
		informerFactory: inFactory,
		dynamicFactory:  dyFactory,

		subscriber: make(map[string]EventCreator),
		stopCtx:    ctx,
	}
}

func (b *builder) Name() string {
	return logPrefix
}

func (b *builder) CanProcess(msg interface{}) bool {
	switch msg.(type) {
	case *k8sproxy.SubscribeResource:
		return true
	}
	return false
}

func (b *builder) Process(ctx actor.Context) (response interface{}, err error) {
	switch msg := ctx.Message().(type) {
	case *k8sproxy.SubscribeResource:
		return b.subscribeResource(msg, ctx)
	default:
		return nil, nil
	}
}

func (b *builder) subscribeResource(subInfo *k8sproxy.SubscribeResource, ctx actor.Context) (interface{}, error) {
	if ctx.Sender() == nil {
		return nil, fmt.Errorf("%s recorder fail, due to an empty recorder", logPrefix)
	}

	eventCreator, ok := b.subscriber[ctx.Sender().String()]
	if !ok {
		eventCreator = NewEventCreator(ctx)
		b.subscriber[ctx.Sender().String()] = eventCreator
	}
	event := eventCreator.CreateSubscribeEvent(subInfo)

	b.dynamicFactory.ForResource(k8sproxy.ConvertGVR(*subInfo.GVR)).Informer().AddEventHandler(event)
	b.dynamicFactory.Start(b.stopCtx.Done())
	return k8sproxy.NewSubscribeConfirm(*subInfo), nil
}

func (b *builder) Initial(ctx actor.Context) error {
	b.informerFactory.Start(b.stopCtx.Done())
	b.dynamicFactory.Start(b.stopCtx.Done())
	b.tieContext = ctx
	return nil
}
