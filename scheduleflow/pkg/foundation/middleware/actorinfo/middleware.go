package actorinfo

import (
	"context"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/facebookgo/inject"
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

	return func(next actor.ReceiverFunc) actor.ReceiverFunc {
		return produceReceiverFunc(next, cfg)
	}
}

func produceReceiverFunc(next actor.ReceiverFunc, cfg config) actor.ReceiverFunc {
	return func(rCtx actor.ReceiverContext, env *actor.MessageEnvelope) {
		message := env.Message
		switch message.(type) {
		case *actor.Started:
			injectActor(rCtx, cfg)

		case *actor.Stopping:
			cfg.stop()
		}
		next(rCtx, env)
	}
}

func injectActor(rCtx actor.ReceiverContext, cfg config) {
	graph := inject.Graph{}
	err := graph.Provide(
		&inject.Object{Value: newInformation(rCtx, cfg.ctx)},
		&inject.Object{Value: rCtx.Actor(), Name: "_tobeInjected"},
	)
	if err != nil {
		logrus.Errorf("can not inject actor information due to error: %v", err)
		return
	}

	err = graph.Populate()
	if err != nil {
		logrus.Errorf("can not inject actor information due to error: %v", err)
	}
}
