package dynamic_api

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/k8sproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
	"k8s.io/client-go/dynamic"
)

const logPrefix = "[KubernetesAPI]"

type dynamicAPI struct {
	client dynamic.Interface
}

// String interface string
func (api *dynamicAPI) String() string {
	return logPrefix
}

func New(client dynamic.Interface) processor.ActorProcessor {
	return &dynamicAPI{
		client: client,
	}
}

// Name
func (api *dynamicAPI) Name() string {
	return "DynamicAPI"
}

// CanProcess trigger processor processing
func (api *dynamicAPI) CanProcess(msg interface{}) bool {
	switch msg.(type) {
	case *k8sproxy.Create, *k8sproxy.Delete, *k8sproxy.Get,
		*k8sproxy.Update, *k8sproxy.List, *k8sproxy.UpdateStatus,
		*k8sproxy.Patch:
		return true
	default:
		return false
	}
}

// Process deal with message
func (api *dynamicAPI) Process(ctx actor.Context) (interface{}, error) {
	switch msg := ctx.Message().(type) {
	case *k8sproxy.Get:
		return api.get(msg)
	case *k8sproxy.Create:
		return api.create(msg)
	case *k8sproxy.List:
		return api.list(msg)
	case *k8sproxy.Delete:
		return api.delete(msg)
	case *k8sproxy.Patch:
		return api.patch(msg)
	case *k8sproxy.Update:
		return api.update()
	case *k8sproxy.UpdateStatus:
		return api.updateStatus()
	}
	return nil, nil
}
