package actorinfo

import (
	"context"
	"time"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/sirupsen/logrus"
)

type ActorBaseInformation interface {
	Self() *actor.PID
	Parent() *actor.PID
	Actor() actor.Actor
	ActorSystem() *actor.ActorSystem
	Context() actor.Context
	Goroutine() context.Context
}

type config struct {
	ctx     context.Context
	stop    context.CancelFunc
	waiting bool
}

type Option func(*config)

func WithGoroutineContext(ctx context.Context, stop context.CancelFunc) Option {
	return func(c *config) {
		c.ctx = ctx
		c.stop = stop
	}
}

func WithWaitingParentInitial() Option {
	return func(c *config) {
		c.waiting = true
	}
}

type baseImplement struct {
	self      *actor.PID
	parent    *actor.PID
	actor     actor.Actor
	system    *actor.ActorSystem
	context   actor.Context
	goroutine context.Context
}

func newInformation(c actor.ReceiverContext, ctx context.Context) ActorBaseInformation {
	actorCtx := c.(actor.Context)
	return &baseImplement{
		self:      c.Self(),
		parent:    c.Parent(),
		actor:     c.Actor(),
		system:    c.ActorSystem(),
		context:   actorCtx,
		goroutine: ctx,
	}
}

func (imp *baseImplement) Self() *actor.PID {
	return imp.self
}

func (imp *baseImplement) Parent() *actor.PID {
	return imp.parent
}

func (imp *baseImplement) Actor() actor.Actor {
	return imp.actor
}

func (imp *baseImplement) ActorSystem() *actor.ActorSystem {
	return imp.system
}

func (imp *baseImplement) Context() actor.Context {
	return imp.context
}

func (imp *baseImplement) Goroutine() context.Context {
	return imp.goroutine
}

type querySystemInitialed struct{}

type systemInitialed struct{}

// NewMiddlewareProducer receives option to initial middleware
func NewMiddlewareProducer(opts ...Option) actor.ReceiverMiddleware {
	cfg := config{ctx: context.Background()}
	for _, opt := range opts {
		opt(&cfg)
	}

	started := func(rCtx actor.ReceiverContext, env *actor.MessageEnvelope) bool {
		if cfg.waiting && rCtx.Parent() != nil {
			ctx := rCtx.(actor.Context)
			future := ctx.RequestFuture(rCtx.Parent(), &querySystemInitialed{}, time.Second)
			result, err := future.Result()
			if err != nil {
				logrus.Errorf("querySystemInitialed timeout")
			}

			_, ok := result.(*systemInitialed)
			if !ok {
				logrus.Errorf("expected *systemInitialed but get %T", result)
			}
		}

		err := utils.InjectActor(rCtx, utils.NewInjectorItem("", newInformation(rCtx, cfg.ctx)))
		if err != nil {
			logrus.Error(err)
		}
		return false
	}

	stopping := func(rCtx actor.ReceiverContext, env *actor.MessageEnvelope) bool {
		if cfg.stop != nil {
			cfg.stop()
		}
		return false
	}

	unblockChildren := func(rCtx actor.ReceiverContext, env *actor.MessageEnvelope) bool {
		switch env.Message.(type) {
		case *querySystemInitialed:
			ctx := rCtx.(actor.Context)
			ctx.Send(env.Sender, &systemInitialed{})
			return true
		}
		return false
	}

	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(started).BuildOnStopping(stopping).
		BuildOnOther(unblockChildren).ProduceReceiverMiddleware()
}
