package processor

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"
	"github.com/sirupsen/logrus"
)

// NewMiddlewareProducer produces a Manager middleware
func NewMiddlewareProducer(initializers ...ActorProcessorWithInitial) actor.ReceiverMiddleware {
	manager := newManager(initializers...)
	injectManager := func(c actor.ReceiverContext, env *actor.MessageEnvelope) bool {
		err := utils.InjectActor(c, utils.NewInjectorItem("", manager))
		if err != nil {
			logrus.Fatal(err)
			return false
		}
		return false
	}

	initialProcessor := func(c actor.ReceiverContext, env *actor.MessageEnvelope) bool {
		ctx := c.(actor.Context)
		err := manager.Initial(ctx)
		if err != nil {
			logrus.Errorf("initial processor with error: %v", err)
		}
		return false
	}

	managing := func(c actor.ReceiverContext, env *actor.MessageEnvelope) bool {
		ctx := c.(actor.Context)

		_, err := manager.Process(ctx, env)
		if err != nil {
			logrus.Errorf("process results with error: %v", err)
		}
		return false
	}

	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(injectManager).
		BuildOnStartedDefer(initialProcessor).BuildOnOtherDefer(managing).ProduceReceiverMiddleware()
}
