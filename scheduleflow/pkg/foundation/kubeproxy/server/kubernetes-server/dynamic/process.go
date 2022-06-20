package dynamic_api

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
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
	case *kubeproxy.Create, *kubeproxy.Delete, *kubeproxy.Get,
		*kubeproxy.Update, *kubeproxy.List, *kubeproxy.UpdateStatus,
		*kubeproxy.Patch:
		return true
	default:
		return false
	}
}

// Process deal with message
func (api *dynamicAPI) Process(ctx actor.Context, env *actor.MessageEnvelope) (interface{}, error) {
	switch m := env.Message.(type) {
	case *kubeproxy.Get:
		return api.get(m)
	case *kubeproxy.Create:
		return api.create(m)
	case *kubeproxy.List:
		return api.list(m)
	case *kubeproxy.Delete:
		return api.delete(m)
	case *kubeproxy.Patch:
		return api.patch(m)
	case *kubeproxy.Update:
		return api.update(m)
	case *kubeproxy.UpdateStatus:
		return api.updateStatus(m)
	}
	return nil, nil
}
