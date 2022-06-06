package reserve

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/k8sproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/k8sproxy/informer"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
)

type Factory interface {
	CreateReserve(gvr k8sproxy.GroupVersionResource) processor.ActorProcessor
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

func (fac *templateFactory) CreateReserve(gvr k8sproxy.GroupVersionResource) processor.ActorProcessor {
	return nil
}

type reserve[R any] struct {
	gvr k8sproxy.GroupVersionResource
}

func (r *reserve[R]) Name() string {
	return r.gvr.String()
}

func (r *reserve[R]) CanProcess(msg interface{}) bool {
	return false
}

func (r *reserve[R]) Process(ctx actor.Context) (interface{}, error) {
	return nil, nil
}
