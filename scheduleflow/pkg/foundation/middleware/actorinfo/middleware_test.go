package actorinfo

import (
	"fmt"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
)

type hello struct {
	Who string
}

type helloActor struct {
	Base ActorBaseInformation `inject:""`

	t *testing.T
}

func (h *helloActor) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *actor.Started:
		if h.Base == nil {
			h.t.Fatal("inject fail")
		}

		if h.Base.Self() == nil {
			h.t.Fatal("inject pid fail")
		}

		if h.Base.Self() != ctx.Self() {
			h.t.Fatal("inject a wrong pid")
		}

	case *hello:
		fmt.Printf("Hello %v\n", msg.Who)
		fmt.Printf("base information %v", h.Base.Self())
	}
}

func TestBaseImplement_Actor(t *testing.T) {
	system := actor.NewActorSystem()
	rootContext := system.Root
	props := actor.PropsFromProducer(func() actor.Actor {
		return &helloActor{t: t}
	}, actor.WithReceiverMiddleware(NewMiddlewareProducer()))
	pid := rootContext.Spawn(props)
	rootContext.Send(pid, &hello{Who: "Roger"})
	time.Sleep(100 * time.Millisecond)
}
