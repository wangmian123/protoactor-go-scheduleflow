package processor

import "github.com/asynkron/protoactor-go/actor"

type ActorProcessor interface {
	Name() string
	CanProcess(msg interface{}) bool
	Process(ctx actor.Context) (interface{}, error)
}

type ActorProcessorWithInitial interface {
	ActorProcessor
	Initial(ctx actor.Context) error
}

// Manager is interface which and
type Manager interface {
	ActorProcessorWithInitial
	AddProcessor(processors ...ActorProcessor)
	GetProcessor(name string) (ActorProcessor, bool)
}

// managerImplement
type managerImplement struct {
	initials   map[string]ActorProcessorWithInitial
	processors map[string]ActorProcessor
}

func newManager(processors ...ActorProcessorWithInitial) Manager {
	initials := make(map[string]ActorProcessorWithInitial, len(processors))
	for _, p := range processors {
		initials[p.Name()] = p
	}

	return &managerImplement{
		initials:   initials,
		processors: make(map[string]ActorProcessor, len(processors)),
	}
}

func (m *managerImplement) Name() string {
	return "[ProcessorManager]"
}

func (m *managerImplement) CanProcess(msg interface{}) bool {
	for _, p := range m.processors {
		if p.CanProcess(msg) {
			return true
		}
	}
	return false
}

func (m *managerImplement) Process(ctx actor.Context) (interface{}, error) {
	msg := ctx.Message()
	for _, pro := range m.processors {
		if !pro.CanProcess(msg) {
			continue
		}

		resp, err := pro.Process(ctx)
		if err != nil {
			return nil, err
		}

		if ctx.Sender() == nil || resp == nil {
			continue
		}

		ctx.Respond(resp)
	}

	return nil, nil
}

func (m *managerImplement) Initial(ctx actor.Context) error {
	for _, ini := range m.initials {
		err := ini.Initial(ctx)
		if err != nil {
			return err
		}

		m.processors[ini.Name()] = ini
	}
	return nil
}

func (m *managerImplement) AddProcessor(processors ...ActorProcessor) {
	for _, p := range processors {
		m.processors[p.Name()] = p
	}
}

func (m *managerImplement) GetProcessor(name string) (ActorProcessor, bool) {
	processor, ok := m.processors[name]
	return processor, ok
}
