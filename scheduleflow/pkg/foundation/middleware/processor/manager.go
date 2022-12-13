package processor

import (
	"fmt"

	"github.com/asynkron/protoactor-go/actor"
)

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

	for _, p := range m.initials {
		if p.CanProcess(msg) {
			return true
		}
	}

	return false
}

func (m *managerImplement) Process(ctx actor.Context, env *actor.MessageEnvelope) (interface{}, error) {
	for _, pro := range m.processors {
		if !pro.CanProcess(env.Message) {
			continue
		}

		resp, err := pro.Process(ctx, env)
		if err != nil {
			return nil, err
		}

		if env.Sender == nil || resp == nil {
			continue
		}

		ctx.Send(env.Sender, resp)
	}

	for _, pro := range m.initials {
		if !pro.CanProcess(env.Message) {
			continue
		}

		resp, err := pro.Process(ctx, env)
		if err != nil {
			return nil, err
		}

		if env.Sender == nil || resp == nil {
			continue
		}

		ctx.Send(env.Sender, resp)
	}

	return nil, nil
}

func (m *managerImplement) Initial(ctx actor.Context) error {
	for _, ini := range m.initials {
		err := ini.Initial(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *managerImplement) AddProcessor(processors ...ActorProcessor) error {
	for _, p := range processors {
		_, ok := m.processors[p.Name()]
		if ok {
			return fmt.Errorf("can not add a process named %s ,due to process existed", p.Name())
		}
		m.processors[p.Name()] = p
	}
	return nil
}

func (m *managerImplement) AddInitialProcessor(processors ...ActorProcessorWithInitial) error {
	for _, p := range processors {
		_, ok := m.initials[p.Name()]
		if ok {
			return fmt.Errorf("can not add a process named %s ,due to process existed", p.Name())
		}
		m.initials[p.Name()] = p
	}
	return nil
}

func (m *managerImplement) RemoveProcessor(processors ...ActorProcessor) {
	for _, p := range processors {
		delete(m.processors, p.Name())
	}
}

func (m *managerImplement) GetProcessor(name string) (ActorProcessor, bool) {
	processor, ok := m.processors[name]
	return processor, ok
}
