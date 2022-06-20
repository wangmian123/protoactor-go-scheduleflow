package actorinfo

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/asynkron/protoactor-go/actor"
)

type hello struct {
	Who string
}

type helloActor struct {
	Base ActorBaseInformation `inject:""`

	t *testing.T
}

func receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case *actor.Started:
		logrus.Infof("receive function start")
	}
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
		ctx.Spawn(actor.PropsFromFunc(receive,
			actor.WithReceiverMiddleware(NewMiddlewareProducer(WithWaitingParentInitial())),
		))

	case *hello:
		logrus.Infof("Hello %v\n", msg.Who)
		logrus.Infof("base information %v", h.Base.Self())
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
	time.Sleep(2 * time.Second)
}
