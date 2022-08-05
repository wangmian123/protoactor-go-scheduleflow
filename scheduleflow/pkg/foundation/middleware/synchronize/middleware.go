package synchronize

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/informer"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/kubernetes"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"
	"github.com/sirupsen/logrus"
)

func NewSynchronizerFactoryMiddlewareProducer() actor.ReceiverMiddleware {
	onStart := func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) bool {
		ctx := c.(actor.Context)
		dyInformer := informer.NewDynamicInformer(c.ActorSystem(), informer.GetLocalClient(c.ActorSystem()))
		fac := NewFactory(dyInformer, kubernetes.NewKubernetesAPIBuilder(ctx))
		err := utils.InjectActor(c, utils.NewInjectorItem("", fac))
		if err != nil {
			logrus.Error(err)
		}
		return false
	}
	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(onStart).ProduceReceiverMiddleware()
}
