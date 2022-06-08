package utils

import (
	"fmt"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/facebookgo/inject"
)

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

type TagInjector interface {
	GetTagName() string
	GetInjectObject() any
}

func InjectActor(rCtx actor.ReceiverContext, targets ...TagInjector) error {
	if len(targets) == 0 {
		return nil
	}

	graph := inject.Graph{}

	err := graph.Provide(
		&inject.Object{Value: rCtx.Actor(), Name: "_tobeInjected"},
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

type ReceiverMiddlewareBuilder interface {
	BuildOnStarted(actor.ReceiverFunc) ReceiverMiddlewareBuilder
	BuildOnStopping(actor.ReceiverFunc) ReceiverMiddlewareBuilder
	BuildOnOther(actor.ReceiverFunc) ReceiverMiddlewareBuilder
	ProduceReceiverMiddleware() actor.ReceiverMiddleware
}

type receiverMiddlewareBuilder struct {
	started  actor.ReceiverFunc
	general  actor.ReceiverFunc
	stopping actor.ReceiverFunc
}

func NewReceiverMiddlewareBuilder() ReceiverMiddlewareBuilder {
	return &receiverMiddlewareBuilder{}
}

func (r *receiverMiddlewareBuilder) BuildOnStarted(started actor.ReceiverFunc) ReceiverMiddlewareBuilder {
	r.started = started
	return r
}

func (r *receiverMiddlewareBuilder) BuildOnStopping(general actor.ReceiverFunc) ReceiverMiddlewareBuilder {
	r.general = general
	return r
}

func (r *receiverMiddlewareBuilder) BuildOnOther(stopping actor.ReceiverFunc) ReceiverMiddlewareBuilder {
	r.stopping = stopping
	return r
}

func (r *receiverMiddlewareBuilder) ProduceReceiverMiddleware() actor.ReceiverMiddleware {
	return func(next actor.ReceiverFunc) actor.ReceiverFunc {
		return func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) {
			switch envelope.Message.(type) {
			case *actor.Started:
				if r.started != nil {
					r.started(c, envelope)
				}

			case *actor.Stopping, *actor.Stop:
				if r.stopping != nil {
					r.stopping(c, envelope)
				}

			default:
				if r.general != nil {
					r.general(c, envelope)
				}
			}
			next(c, envelope)
		}
	}
}
