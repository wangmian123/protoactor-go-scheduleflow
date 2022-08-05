package operator

import (
	"fmt"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/informer"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/processor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"
	"k8s.io/client-go/dynamic"
)

type operatorBuilder struct {
	informer.DynamicInformer
	dynamic.Interface
	localInformer *actor.PID
}

func NewOperatorBuilder(dynamicInformer informer.DynamicInformer, sys *actor.ActorSystem, client dynamic.Interface) ServerOperatorBuilder {
	return &operatorBuilder{
		DynamicInformer: dynamicInformer,
		localInformer:   informer.GetLocalClient(sys),
		Interface:       client,
	}
}

func (builder *operatorBuilder) CreateResourceOperator(resource *kubeproxy.GroupVersionResource) (processor.ActorProcessor, error) {
	operator := newOperatorAPI(resource, builder.Interface)
	operatableStores, err := builder.SetResourceHandler(informer.NewDynamicSubscribe(builder.localInformer,
		kubeproxy.ConvertGVR(resource.DeepCopy()), informer.WithDynamicInformerHandler(&utils.HandlerFunc{
			Creating: operator.informResourceAdd,
			Deleting: operator.informResourceDelete,
			Updating: operator.informResourceUpdate,
		})))

	if err != nil {
		return nil, err
	}

	if len(operatableStores) != 1 {
		return nil, fmt.Errorf("set resource handler returns %d stores, expected 1", len(operatableStores))
	}

	operator.store = operatableStores[0]
	return operator, nil
}
