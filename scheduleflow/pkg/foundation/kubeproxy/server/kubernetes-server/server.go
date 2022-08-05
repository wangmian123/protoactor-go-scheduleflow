package kubernetes_server

import (
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/server/kubernetes-server/operator"

	"github.com/sirupsen/logrus"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/actorinfo"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/informer"
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

	dynamicClient   dynamic.Interface
	operatorBuilder operator.ServerOperatorBuilder
}

func New(cfg *rest.Config) *actor.Props {
	client := dynamic.NewForConfigOrDie(cfg)
	return actor.PropsFromProducer(func() actor.Actor {
		return &kubernetesAPI{
			dynamicClient: client,
		}
	}, actor.WithReceiverMiddleware(
		actorinfo.NewMiddlewareProducer(),
		processor.NewMiddlewareProducer(),
		informer.NewDynamicClientMiddlewareProducer(),
	))
}

func (api *kubernetesAPI) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *actor.Started:
		logrus.Infof("=======%s start=======", logPrefix)
		api.operatorBuilder = operator.NewOperatorBuilder(api.DynamicInformer, api.ActorSystem(), api.dynamicClient)
	case kubeproxy.KubernetesAPIBase:
		_, ok := api.Manager.GetProcessor(msg.GetGVR().String())
		if ok {
			return
		}

		if msg.GetGVR() == nil {
			logrus.Errorf("%s can not create resource operator due to GVR is nil", logPrefix)
			return
		}

		p, err := api.operatorBuilder.CreateResourceOperator(msg.GetGVR())
		if err != nil {
			logrus.Errorf("%s can not create resource opecrator: %v due to %v", logPrefix, msg.GetGVR().String(), err)
			return
		}

		if p.Name() != msg.GetGVR().String() {
			logrus.Errorf("%s can not create resource opecrator: %v due to created operator name not matched",
				logPrefix, msg.GetGVR().String())
			return
		}
		logrus.Infof("%s add resource operator %s", logPrefix, msg.GetGVR().String())
		api.Manager.AddProcessor(p)
	}
}
