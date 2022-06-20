package reserve

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/kubeproxy/informer"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
)

type Factory interface {
	CreateReserve(gvr kubeproxy.GroupVersionResource) processor.ActorProcessor
}

type templateFactory struct {
	localInformerServer *actor.PID
	informer            informer.DynamicInformer
}

func NewFactory(localInformerServer *actor.PID) Factory {
	return &templateFactory{
		localInformerServer: localInformerServer,
	}
}

func (fac *templateFactory) CreateReserve(gvr kubeproxy.GroupVersionResource) processor.ActorProcessor {
	return nil
}

type reserve[R any] struct {
	gvr kubeproxy.GroupVersionResource
}

func (r *reserve[R]) Name() string {
	return r.gvr.String()
}

func (r *reserve[R]) CanProcess(msg interface{}) bool {
	return false
}

func (r *reserve[R]) Process(ctx actor.Context, env *actor.MessageEnvelope) (interface{}, error) {
	return nil, nil
}
