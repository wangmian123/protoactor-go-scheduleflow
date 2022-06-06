package processor

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/facebookgo/inject"
	"github.com/sirupsen/logrus"
)

// NewMiddlewareProducer produces a Manager middleware
func NewMiddlewareProducer(initializers ...ActorProcessorWithInitial) actor.ReceiverMiddleware {
	return func(next actor.ReceiverFunc) actor.ReceiverFunc {
		fn := func(c actor.ReceiverContext, env *actor.MessageEnvelope) {
			message := env.Message
			switch message.(type) {
			case *actor.Started:
				manager := newManager(initializers...)
				graph := inject.Graph{}
				err := graph.Provide(
					&inject.Object{Value: manager},
					&inject.Object{Value: c.Actor(), Name: "_tobeInjected"})
				if err != nil {
					logrus.Errorf("can not inject processor manager due to error: %v", err)
					return
				}

				err = graph.Populate()
				if err != nil {
					logrus.Errorf("can not inject processor manager due to error: %v", err)
					return
				}

				ctx := c.(actor.Context)
				err = manager.Initial(ctx)
				if err != nil {
					logrus.Errorf("initial processor with error: %v", err)
					return
				}
			default:
				ctx := c.(actor.Context)
				manager, ok := c.Actor().(Manager)
				if !ok {
					return
				}

				if manager.CanProcess(message) {
					_, err := manager.Process(ctx)
					if err != nil {
						logrus.Errorf("process results with error: %v", err)
					}
				}
			}

			next(c, env)
		}
		return fn
	}
}
