package apiserver

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/apiregistry"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
)

const apiServer = "APIServer"

func GetPID(server string) *actor.PID {
	return actor.NewPID(server, apiServer)
}

type HTTPRegistryByDefault interface {
	apiregistry.HTTPRegistryByDefault
}

type HTTPRegistry interface {
	apiregistry.HTTPRegistry
}

type HTTPRegistryBuilder interface {
	apiregistry.HTTPRegistryBuilder
}

// NewMiddleware inject an api server registry interface, and if no api server had
// been found, then creating a server with no mongo recorder.
func NewMiddleware() actor.ReceiverMiddleware {
	started := func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) bool {
		apiPID := actor.NewPID(c.Self().GetAddress(), apiServer)
		_, ok := c.ActorSystem().ProcessRegistry.Get(apiPID)

		if !ok {
			var err error
			apiPID, err = c.ActorSystem().Root.SpawnNamed(apiregistry.New(nil), apiServer)
			if err != nil {
				logrus.Errorf("spawn api server fail due to %v", err)
				return false
			}
		}

		err := utils.InjectActor(c, utils.NewInjectorItem("",
			apiregistry.NewRegistry(c.(actor.Context), apiPID)))
		if err != nil {
			logrus.Error(err)
		}
		return false
	}
	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(started).
		ProduceReceiverMiddleware()
}

// NewAPIServer create an api server with mongo recorder.
func NewAPIServer(db *mongo.Collection) actor.ReceiverMiddleware {
	started := func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) bool {
		apiPID := actor.NewPID(c.Self().GetAddress(), apiServer)
		_, ok := c.ActorSystem().ProcessRegistry.Get(apiPID)

		if !ok {
			var err error
			apiPID, err = c.ActorSystem().Root.SpawnNamed(apiregistry.New(db), apiServer)
			if err != nil {
				logrus.Errorf("spawn api server fail due to %v", err)
				return false
			}
		}
		return false
	}
	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(started).
		ProduceReceiverMiddleware()
}
