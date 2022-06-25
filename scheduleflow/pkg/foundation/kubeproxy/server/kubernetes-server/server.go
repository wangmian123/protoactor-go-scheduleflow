package kubernetes_server

import (
	"sync"

	"github.com/sirupsen/logrus"

	dynamic_api "github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/server/kubernetes-server/dynamic"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/actorinfo"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/kubeproxy/informer"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

const logPrefix = "[KubernetesAPI]"

type Option func(api *kubernetesAPI)

type kubernetesAPI struct {
	actorinfo.ActorBaseInformation `inject:""`
	informer.DynamicInformer       `inject:""`

	Manager processor.Manager `inject:""`

	dynamicClient dynamic.Interface
	once          *sync.Once
}

func New(cfg *rest.Config) *actor.Props {
	client := dynamic.NewForConfigOrDie(cfg)
	return actor.PropsFromProducer(func() actor.Actor {
		return &kubernetesAPI{
			dynamicClient: client,
			once:          &sync.Once{},
		}
	}, actor.WithReceiverMiddleware(
		actorinfo.NewMiddlewareProducer(),
		processor.NewMiddlewareProducer(),
		informer.NewDynamicClientMiddlewareProducer(),
	))
}

func (api *kubernetesAPI) Receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case *actor.Started:
		logrus.Infof("=======%s start=======", logPrefix)
		api.Manager.AddProcessor(dynamic_api.New(api.dynamicClient))
	}
}
