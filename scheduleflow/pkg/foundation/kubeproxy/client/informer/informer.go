package informer

import (
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer/dispacher"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer/recorder"
	cmap "github.com/orcaman/concurrent-map"

	"github.com/sirupsen/logrus"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer/fundamental"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/actorinfo"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
)

type informer struct {
	actorinfo.ActorBaseInformation `inject:""`
	processor.Manager              `inject:""`

	eventMap fundamental.SubscribeEventMap
}

func New() *actor.Props {

	eventMap := fundamental.SubscribeEventMap{ConcurrentMap: cmap.New[*fundamental.SubscriberInformationMap]()}
	producer := func() actor.Actor {
		return &informer{
			eventMap: eventMap,
		}
	}

	return actor.PropsFromProducer(producer,
		actor.WithReceiverMiddleware(
			actorinfo.NewMiddlewareProducer(),
			processor.NewMiddlewareProducer(recorder.New(eventMap), dispacher.New(eventMap)),
		),
	)
}

func (inf *informer) Receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case *actor.Started:
		logrus.Infof("[%s] start", inf.Self().Id)
	}
}
