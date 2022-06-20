package utils

import (
	"fmt"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/facebookgo/inject"
	"github.com/hashicorp/go-uuid"
)

type TagInjector interface {
	GetTagName() string
	GetInjectObject() any
}

// ReceiverFuncWithBreak return middleware ReceiverFunc function with bool whether to break the entire message
// process. So be careful when return ture.
type ReceiverFuncWithBreak func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) bool

type ReceiverMiddlewareBuilder interface {
	BuildOnStarted(ReceiverFuncWithBreak) ReceiverMiddlewareBuilder
	BuildOnStopping(ReceiverFuncWithBreak) ReceiverMiddlewareBuilder
	BuildOnOther(ReceiverFuncWithBreak) ReceiverMiddlewareBuilder
	BuildOnStartedDefer(ReceiverFuncWithBreak) ReceiverMiddlewareBuilder
	BuildOnStoppingDefer(ReceiverFuncWithBreak) ReceiverMiddlewareBuilder
	BuildOnOtherDefer(ReceiverFuncWithBreak) ReceiverMiddlewareBuilder
	ProduceReceiverMiddleware() actor.ReceiverMiddleware
}

type injector struct {
	tagName string
	obj     interface{}
}

func (i *injector) GetTagName() string {
	return i.tagName
}

func (i *injector) GetInjectObject() any {
	return i.obj
}

func NewInjectorItem(name string, obj interface{}) TagInjector {
	return &injector{
		tagName: name,
		obj:     obj,
	}
}

func InjectActor(rCtx actor.ReceiverContext, targets ...TagInjector) error {
	if len(targets) == 0 {
		return nil
	}
	id, err := uuid.GenerateUUID()
	if err != nil {
		return err
	}

	graph := inject.Graph{}

	err = graph.Provide(
		&inject.Object{Value: rCtx.Actor(), Name: id},
	)

	for _, tar := range targets {
		object := tar.GetInjectObject()
		name := tar.GetTagName()

		if object == nil {
			return fmt.Errorf("inject target is nil named: %s", name)
		}

		err = graph.Provide(
			&inject.Object{Value: object, Name: name},
		)
	}

	if err != nil {
		return fmt.Errorf("can not inject actor information due to error: %v", err)
	}

	err = graph.Populate()
	if err != nil {
		return fmt.Errorf("can not inject actor information due to error: %v", err)
	}

	return nil
}

type receiverMiddlewareBuilder struct {
	started  ReceiverFuncWithBreak
	general  ReceiverFuncWithBreak
	stopping ReceiverFuncWithBreak

	startedDefer  ReceiverFuncWithBreak
	generalDefer  ReceiverFuncWithBreak
	stoppingDefer ReceiverFuncWithBreak
}

func NewReceiverMiddlewareBuilder() ReceiverMiddlewareBuilder {
	return &receiverMiddlewareBuilder{}
}

func (r *receiverMiddlewareBuilder) BuildOnStarted(started ReceiverFuncWithBreak) ReceiverMiddlewareBuilder {
	r.started = started
	return r
}

func (r *receiverMiddlewareBuilder) BuildOnStopping(stopping ReceiverFuncWithBreak) ReceiverMiddlewareBuilder {
	r.stopping = stopping
	return r
}

func (r *receiverMiddlewareBuilder) BuildOnOther(general ReceiverFuncWithBreak) ReceiverMiddlewareBuilder {
	r.general = general
	return r
}

func (r *receiverMiddlewareBuilder) BuildOnStartedDefer(started ReceiverFuncWithBreak) ReceiverMiddlewareBuilder {
	r.startedDefer = started
	return r
}

func (r *receiverMiddlewareBuilder) BuildOnStoppingDefer(stopping ReceiverFuncWithBreak) ReceiverMiddlewareBuilder {
	r.stoppingDefer = stopping
	return r
}

func (r *receiverMiddlewareBuilder) BuildOnOtherDefer(general ReceiverFuncWithBreak) ReceiverMiddlewareBuilder {
	r.generalDefer = general
	return r
}

func (r *receiverMiddlewareBuilder) ProduceReceiverMiddleware() actor.ReceiverMiddleware {
	return func(next actor.ReceiverFunc) actor.ReceiverFunc {
		return func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) {
			switch envelope.Message.(type) {
			case *actor.Started:
				if r.started != nil {
					br := r.started(c, envelope)
					if br {
						return
					}
				}

			case *actor.Stopping, *actor.Stop:
				if r.stopping != nil {
					br := r.stopping(c, envelope)
					if br {
						return
					}
				}

			default:
				if r.general != nil {
					br := r.general(c, envelope)
					if br {
						return
					}
				}
			}
			next(c, envelope)

			switch envelope.Message.(type) {
			case *actor.Started:
				if r.startedDefer != nil {
					br := r.startedDefer(c, envelope)
					if br {
						return
					}
				}

			case *actor.Stopping, *actor.Stop:
				if r.stoppingDefer != nil {
					br := r.stoppingDefer(c, envelope)
					if br {
						return
					}
				}

			default:
				if r.generalDefer != nil {
					br := r.generalDefer(c, envelope)
					if br {
						return
					}
				}
			}
		}
	}
}
