package actorinfo

import (
	"context"

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
	ctx  context.Context
	stop context.CancelFunc
}

type Option func(*config)

func WithGoroutineContext(ctx context.Context, stop context.CancelFunc) Option {
	return func(c *config) {
		c.ctx = ctx
		c.stop = stop
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

// NewMiddlewareProducer receives option to initial middleware
func NewMiddlewareProducer(opts ...Option) actor.ReceiverMiddleware {
	cfg := config{ctx: context.Background()}
	for _, opt := range opts {
		opt(&cfg)
	}

	started := func(rCtx actor.ReceiverContext, env *actor.MessageEnvelope) {
		err := utils.InjectActor(rCtx, utils.NewInjectorItem("", newInformation(rCtx, cfg.ctx)))
		if err != nil {
			logrus.Error(err)
			return
		}
	}

	stopping := func(rCtx actor.ReceiverContext, env *actor.MessageEnvelope) {
		if cfg.stop != nil {
			cfg.stop()
		}
	}

	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(started).BuildOnStopping(stopping).ProduceReceiverMiddleware()
}
