package informer

import (
	"fmt"
	"time"

	informer_server "github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/server/informer-server"
	"k8s.io/client-go/rest"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer/fundamental"
	"github.com/sirupsen/logrus"
)

const (
	logPrefix      = "[ClusterInformerProxy]"
	timeout        = 30 * time.Second
	informerServer = "InformerServer"
	informerClient = "InformerClient"

	DefaultQPS   = 100
	DefaultBurst = 500
)

// middlewareProducer
type middlewareProducer[R any] struct {
	store     *k8sOperableResourceStore[R]
	injectTag string
}

func NewClientMiddlewareProducer[R any](constraint StoreConstraint[R], tagName string) *middlewareProducer[R] {
	store := newOperableResourceStore(constraint, 0)
	return &middlewareProducer[R]{
		store:     store,
		injectTag: tagName,
	}
}

func (mid *middlewareProducer[R]) ProduceSubscribeMiddleware(target *actor.PID, subRes SubscribeResource,
	handler fundamental.ResourceEventHandlerFuncs[R]) actor.ReceiverMiddleware {
	mid.store.handlers = append(mid.store.handlers, handler)
	subscribeEvent := fundamental.SubscribeResourceFrom[R]{
		Source: GetPID(target.GetAddress()),
		Resource: &kubeproxy.SubscribeResource{
			GVR:        kubeproxy.NewGroupVersionResource(subRes.GVR),
			ActionCode: 0,
			Option:     &kubeproxy.SubscribeOption{RateLimitation: subRes.Option.RateLimitation},
		},
		Handler: mid.store,
	}

	started := func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) bool {
		err := mid.onStart(c, subscribeEvent)
		if err != nil {
			logrus.Error(err)
		}
		return false
	}

	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(started).ProduceReceiverMiddleware()
}

func (mid *middlewareProducer[R]) onStart(c actor.ReceiverContext, subscribeEvent fundamental.SubscribeResourceFrom[R]) error {
	err := utils.InjectActor(c, utils.NewInjectorItem(mid.injectTag, mid.store))
	if err != nil {
		return err
	}

	ctx := c.(actor.Context)

	informerPid := actor.NewPID(c.Self().Address, informerClient)
	_, ok := c.ActorSystem().ProcessRegistry.Get(informerPid)

	if !ok {
		informerPid, err = ctx.ActorSystem().Root.SpawnNamed(informer.New(), informerClient)
		if err != nil {
			return fmt.Errorf("spawn informer-server proxy fail due to %v", err)
		}
	}

	future := ctx.RequestFuture(informerPid, &subscribeEvent, timeout)
	result, err := future.Result()
	if err != nil {
		return fmt.Errorf("subscribe error due to %v", err)
	}

	ack, ok := result.(*fundamental.SubscribeRespond)
	if !ok {
		return fmt.Errorf("subscribe respond type expect *fundamental.SubscribeRespond, but get %T", result)
	}

	if ack.Code != fundamental.Success {
		return fmt.Errorf("subcribe event with error %s", ack.Message)
	}
	return nil
}

func NewDynamicClientMiddlewareProducer(opts ...Option) actor.ReceiverMiddleware {
	producer := &config{}
	for _, opt := range opts {
		opt(producer)
	}

	started := func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) bool {
		informerPid := actor.NewPID(c.Self().GetAddress(), informerClient)
		_, ok := c.ActorSystem().ProcessRegistry.Get(informerPid)

		if !ok {
			var err error
			informerPid, err = c.ActorSystem().Root.SpawnNamed(informer.New(), informerClient)
			if err != nil {
				logrus.Errorf("spawn informer-server proxy fail due to %v", err)
				return false
			}
		}

		err := utils.InjectActor(c, utils.NewInjectorItem(producer.tagName, NewDynamicInformer(c.ActorSystem(), informerPid)))
		if err != nil {
			logrus.Error(err)
		}
		return false
	}
	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(started).ProduceReceiverMiddleware()
}

func GetPID(server string) *actor.PID {
	return actor.NewPID(server, informerServer)
}

func GetLocalClient(sys *actor.ActorSystem) *actor.PID {
	return actor.NewPID(sys.Address(), informerClient)
}

func NewServerMiddlewareProducer(kubeconfig *rest.Config, syncInterval time.Duration, opts ...Option) actor.ReceiverMiddleware {
	cfg := newConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	kubeconfig.Burst = cfg.burst
	kubeconfig.QPS = cfg.qps
	serverProps := informer_server.New(kubeconfig, informer_server.WithSyncInterval(syncInterval))
	started := func(rCtx actor.ReceiverContext, env *actor.MessageEnvelope) bool {
		informerPid := actor.NewPID(rCtx.Self().Address, informerServer)
		_, ok := rCtx.ActorSystem().ProcessRegistry.Get(informerPid)
		if ok {
			return false
		}

		_, err := rCtx.ActorSystem().Root.SpawnNamed(serverProps, informerServer)
		if err != nil {
			logrus.Fatalf("can not initial informer server due to %v", err)
		}
		return false
	}

	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(started).ProduceReceiverMiddleware()
}
