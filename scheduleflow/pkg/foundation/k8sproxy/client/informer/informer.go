package informer

import (
	"sync"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/k8sproxy/client/informer/dispacher"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/k8sproxy/client/informer/fundamental"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/k8sproxy/client/informer/recorder"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/actorinfo"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
)

const (
	logPrefix = fundamental.LogPrefix
)

var once sync.Once

type informer struct {
	actorinfo.ActorBaseInformation `inject:""`

	manager  processor.Manager `inject:""`
	eventMap fundamental.SubscribeEventMap
}

func New() *actor.Props {

	eventMap := cmap.New[fundamental.SubscriberInformationMap]()
	producer := func() actor.Actor {
		return &informer{
			eventMap: eventMap,
		}
	}

	return actor.PropsFromProducer(producer,
		actor.WithReceiverMiddleware(
			actorinfo.NewMiddlewareProducer(),
			processor.NewMiddlewareProducer(),
		),
	)
}

func (inf *informer) Receive(_ actor.Context) {
	once.Do(func() {
		logrus.Infof("%s start", logPrefix)
		inf.manager.AddProcessor(
			recorder.New(inf.eventMap),
			dispacher.New(inf.eventMap),
		)
	})
}
