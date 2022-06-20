package informer_server

import (
	"context"
	"time"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/server/informer-server/subscribe"

	"github.com/sirupsen/logrus"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/actorinfo"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
	"k8s.io/client-go/rest"
)

const (
	logPrefix = "[InformerServer]"

	defaultSyncInterval = 30 * time.Second
)

var defaultConfig informerConfig

func init() {
	defaultConfig = informerConfig{
		syncInterval: defaultSyncInterval,
	}
}

type informerConfig struct {
	syncInterval time.Duration
}

type Option func(config *informerConfig)

func WithSyncInterval(interval time.Duration) Option {
	return func(config *informerConfig) {
		config.syncInterval = interval
	}
}

type informerServer struct {
	actorinfo.ActorBaseInformation `inject:""`

	Manager processor.Manager `inject:""`

	syncInterval time.Duration
	k8sConfig    *rest.Config

	processorCtx context.Context
	stop         context.CancelFunc
}

func New(k8sConfig *rest.Config, opts ...Option) *actor.Props {
	for _, opt := range opts {
		opt(&defaultConfig)
	}

	producer := func() actor.Actor {
		return &informerServer{
			syncInterval: defaultConfig.syncInterval,
			k8sConfig:    k8sConfig,
		}
	}

	rootCtx, cancel := context.WithCancel(context.Background())

	props := actor.PropsFromProducer(producer,
		actor.WithReceiverMiddleware(
			actorinfo.NewMiddlewareProducer(actorinfo.WithGoroutineContext(rootCtx, cancel)),
			processor.NewMiddlewareProducer(subscribe.NewInformerBuilder(k8sConfig, defaultConfig.syncInterval, rootCtx)),
		),
	)

	return props
}

func (inf *informerServer) Receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case *actor.Started:
		logrus.Infof("=======%s start=======", logPrefix)
	default:
	}
}