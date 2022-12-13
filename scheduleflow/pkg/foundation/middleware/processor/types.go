package processor

import "github.com/asynkron/protoactor-go/actor"

type ActorProcessor interface {
	Name() string
	CanProcess(msg interface{}) bool
	Process(ctx actor.Context, env *actor.MessageEnvelope) (interface{}, error)
}

type AsyncProcessor interface {
	ActorProcessor
	AsyncProcess(ctx actor.Context, env *actor.MessageEnvelope) (interface{}, error)
}

type ActorProcessorWithInitial interface {
	ActorProcessor
	Initial(ctx actor.Context) error
}

// Manager is interface which and
type Manager interface {
	ActorProcessorWithInitial
	AddProcessor(processors ...ActorProcessor) error
	AddInitialProcessor(processors ...ActorProcessorWithInitial) error
	GetProcessor(name string) (ActorProcessor, bool)
}
