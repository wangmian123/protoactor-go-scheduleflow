package operator

import (
	"sync"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
)

type ServerOperatorBuilder interface {
	CreateResourceOperator(resource *kubeproxy.GroupVersionResource) (processor.ActorProcessor, error)
}

type resourceCond struct {
	*sync.Cond
	resourceLock *sync.RWMutex
}

func newResourceLock() *resourceCond {
	return &resourceCond{
		Cond:         sync.NewCond(&sync.Mutex{}),
		resourceLock: &sync.RWMutex{},
	}
}
