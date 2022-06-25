package kubernetes

import (
	"github.com/asynkron/protoactor-go/actor"
	kubernetes_server "github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/server/kubernetes-server"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/rest"
)

const apiServer = "KubernetesAPIServer"

func NewAPIClientBuilderMiddlewareProducer(opts ...Option) actor.ReceiverMiddleware {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	started := func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) bool {
		ctx := c.(actor.Context)
		if cfg.ctx != nil {
			ctx = cfg.ctx
		}
		api := NewKubernetesAPIBuilder(ctx)
		err := utils.InjectActor(c, utils.NewInjectorItem(cfg.tagName, api))
		if err != nil {
			logrus.Error(err)
		}
		return false
	}

	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(started).
		ProduceReceiverMiddleware()
}

func NewAPIClientMiddlewareProducer(target *actor.PID, opts ...Option) actor.ReceiverMiddleware {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	started := func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) bool {
		ctx := c.(actor.Context)
		api := NewKubernetesAPI(target, ctx)
		err := utils.InjectActor(c, utils.NewInjectorItem(cfg.tagName, api))
		if err != nil {
			logrus.Error(err)
		}
		return false
	}

	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(started).ProduceReceiverMiddleware()
}

func GetAPI(server string) *actor.PID {
	return actor.NewPID(server, apiServer)
}

func NewAPIServerMiddlewareProducer(kubeconfig *rest.Config) actor.ReceiverMiddleware {
	started := func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) bool {
		apiPID := actor.NewPID(c.Self().Address, apiServer)
		_, ok := c.ActorSystem().ProcessRegistry.Get(apiPID)
		if ok {
			return false
		}

		_, err := c.ActorSystem().Root.SpawnNamed(kubernetes_server.New(kubeconfig), apiServer)
		if err != nil {
			logrus.Fatalf("can not initial kubernetes API server due to %v", err)
		}
		return false
	}

	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(started).ProduceReceiverMiddleware()
}
