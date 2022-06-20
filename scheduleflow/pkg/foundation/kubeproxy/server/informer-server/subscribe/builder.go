package subscribe

import (
	"context"
	"fmt"
	"time"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"

	"github.com/sirupsen/logrus"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	logPrefix = "[InformerServer][InformerBuilder]"
)

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
	case *kubeproxy.SubscribeResource, *kubeproxy.SubscribeResourceFor:
		return true
	}
	return false
}

func (b *builder) Process(ctx actor.Context, env *actor.MessageEnvelope) (response interface{}, err error) {
	switch m := env.Message.(type) {
	case *kubeproxy.SubscribeResource:
		return b.subscribeResource(m, ctx, env.Sender)
	case *kubeproxy.SubscribeResourceFor:
		return b.subscribeResource(m.Resource, ctx, m.Subscriber)
	default:
		return nil, nil
	}
}

func (b *builder) subscribeResource(subInfo *kubeproxy.SubscribeResource, ctx actor.Context, subscriber *actor.PID) (interface{}, error) {
	if subscriber == nil {
		return nil, fmt.Errorf("%s recorder fail, due to an empty recorder", logPrefix)
	}

	logrus.Infof("%s subscriber %s subscribes type %s ", logPrefix, utils.FormActorKey(subscriber), subInfo.GVR.String())

	creator, ok := b.subscriber[subscriber.String()]
	if !ok {
		creator = NewEventCreator(ctx, subscriber)
		b.subscriber[subscriber.String()] = creator
	}
	event := creator.CreateSubscribeEvent(subInfo)

	b.dynamicFactory.ForResource(kubeproxy.ConvertGVR(*subInfo.GVR)).Informer().AddEventHandler(event)
	b.dynamicFactory.Start(b.stopCtx.Done())
	return kubeproxy.NewSubscribeConfirm(*subInfo), nil
}

func (b *builder) Initial(ctx actor.Context) error {
	logrus.Infof("%s start informer factory", logPrefix)
	b.dynamicFactory.Start(b.stopCtx.Done())
	b.tieContext = ctx
	return nil
}
