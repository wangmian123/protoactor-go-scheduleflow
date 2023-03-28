package apiregistry

import (
	"fmt"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/apiregistry/httpserver"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/actorinfo"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
)

const logPrefix = "[APIServer]"

type apiServer struct {
	actorinfo.ActorBaseInformation `inject:""`
	processor.Manager              `inject:""`
	*httpserver.Httpserver

	mongo *mongo.Collection
}

func New(mongo *mongo.Collection) *actor.Props {

	api := &apiServer{
		mongo: mongo,
	}

	return actor.PropsFromProducer(func() actor.Actor {
		return api
	}, actor.WithReceiverMiddleware(
		actorinfo.NewMiddlewareProducer(actorinfo.WithWaitingParentInitial()),
		processor.NewMiddlewareProducer(),
	))
}

func (a *apiServer) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *actor.Started:
		err := a.initial(ctx)
		if err != nil {
			logrus.Fatalf("%s actor initial error due to %v", logPrefix, err)
		}
	case *registerHttp:
		err := a.registerHttpHandler(msg)
		if err != nil {
			ctx.Send(ctx.Sender(), err)
			return
		}
		ctx.Send(ctx.Sender(), &registerHttpRsp{
			group:  msg.group,
			method: msg.method,
			path:   msg.path,
		})
		return
	}
}

func (a *apiServer) initial(ctx actor.Context) error {
	logrus.Infof("======= %s start =======", logPrefix)
	return nil
}

func (a *apiServer) registerHttpHandler(h *registerHttp) error {
	err := a.Httpserver.RegisterHttpHandler(h.group, h.method, h.path, h.handler, h.middlewares...)
	if err != nil {
		return fmt.Errorf("register gin http handler fail due to %v", err)
	}
	logrus.Infof("%s register gin http handler success", logPrefix)
	return nil
}
