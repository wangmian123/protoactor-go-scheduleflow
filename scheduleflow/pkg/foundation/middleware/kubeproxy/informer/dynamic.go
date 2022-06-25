package informer

import (
	"fmt"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer/fundamental"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type DynamicSubscribe struct {
	Target          *actor.PID
	Resource        SubscribeResource
	StoreConstraint StoreConstraint[unstructured.Unstructured]
	Handler         DynamicEventHandler
}

func WithDynamicStoreConstraint(store StoreConstraint[unstructured.Unstructured]) Option {
	return func(c *config) {
		c.storeConstraint = store
	}
}

func WithDynamicInformerHandler(handler DynamicEventHandler) Option {
	return func(c *config) {
		c.handler = handler
	}
}

func NewDynamicSubscribe(target *actor.PID, gvr schema.GroupVersionResource, opts ...Option) *DynamicSubscribe {
	cfg := &config{}
	cfg.storeConstraint = &defaultConstraint{}

	for _, opt := range opts {
		opt(cfg)
	}

	return &DynamicSubscribe{
		Target: target,
		Resource: SubscribeResource{
			GVR:    gvr,
			Option: cfg.SubscribeOption,
		},
		StoreConstraint: cfg.storeConstraint,
		Handler:         cfg.handler,
	}
}

type dynamicHandlerConverter struct {
	handler DynamicEventHandler
}

func convertDynamicEventHandler(handler DynamicEventHandler) fundamental.ResourceEventHandlerFuncs[unstructured.Unstructured] {
	return &dynamicHandlerConverter{
		handler: handler,
	}
}

func (converter *dynamicHandlerConverter) AddFunc(resource unstructured.Unstructured) {
	if converter.handler == nil {
		return
	}
	converter.handler.AddFunc(resource)
}

func (converter *dynamicHandlerConverter) DeleteFunc(resource unstructured.Unstructured) {
	if converter.handler == nil {
		return
	}
	converter.handler.DeleteFunc(resource)
}

func (converter *dynamicHandlerConverter) UpdateFunc(oldResource, newResource unstructured.Unstructured) {
	if converter.handler == nil {
		return
	}
	converter.handler.UpdateFunc(oldResource, newResource)
}

func (converter *dynamicHandlerConverter) Unmarshal(data []byte) (unstructured.Unstructured, error) {
	converted := unstructured.Unstructured{}
	err := json.Unmarshal(data, &converted)
	return converted, err
}

func formGVRKey(pid *actor.PID, gvr schema.GroupVersionResource) string {
	return pid.Address + "/" + pid.Id + "." + gvr.Group + "." + gvr.Version + "." + gvr.Resource
}

type dynamicInformer struct {
	informerPid *actor.PID
	gvrMap      cmap.ConcurrentMap[KubernetesResourceStore[unstructured.Unstructured]]
}

func newDynamicInformer(informerPid *actor.PID) DynamicInformer {
	return &dynamicInformer{
		informerPid: informerPid,
		gvrMap:      cmap.New[KubernetesResourceStore[unstructured.Unstructured]](),
	}
}

func (dy *dynamicInformer) SetResourceHandler(ctx actor.Context, subscribe ...DynamicSubscribe) error {
	if subscribe == nil {
		return fmt.Errorf("%s subscribe info is empty", logPrefix)
	}

	if dy.informerPid == nil {
		informerPid := actor.NewPID(ctx.Self().Address, informerClient)
		_, ok := ctx.ActorSystem().ProcessRegistry.Get(informerPid)
		if !ok {
			var err error
			logrus.Warningf("infomer client dose not started when actor start")
			informerPid, err = ctx.ActorSystem().Root.SpawnNamed(informer.New(), informerClient)
			if err != nil {
				logrus.Fatalf("initial informer client fail due to %v", err)
			}
		}
		dy.informerPid = informerPid
	}

	for _, subInfo := range subscribe {
		resourceStore := newK8sResourceSubscribeWithStore[unstructured.Unstructured](subInfo.StoreConstraint)
		resourceStore.handler = convertDynamicEventHandler(subInfo.Handler)

		subscribeEvent := fundamental.SubscribeResourceFrom[unstructured.Unstructured]{
			Source:   GetPID(subInfo.Target.GetAddress()),
			Resource: convertSubscribeResource(subInfo.Resource),
			Handler:  resourceStore,
		}
		future := ctx.RequestFuture(dy.informerPid, &subscribeEvent, timeout)
		result, err := future.Result()
		if err != nil {
			return fmt.Errorf("subscribe error due to %v", err)
		}

		ack, ok := result.(*fundamental.SubscribeRespond)
		if !ok {
			return fmt.Errorf("subscribe respond type expect *fundamental.SubscribeRespond, but get %T", result)
		}

		if ack.Code != fundamental.Success {
			return fmt.Errorf("subcribe event with error %s", ack.Message)
		}

		_, ok = dy.gvrMap.Get(formGVRKey(subInfo.Target, subInfo.Resource.GVR))
		if ok {
			logrus.Warningf("%s GroupVersionResource has been token, old store will be overwrite", logPrefix)
		}

		dy.gvrMap.Set(formGVRKey(subInfo.Target, subInfo.Resource.GVR), resourceStore)
	}
	return nil
}

func (dy *dynamicInformer) GetResourceStore(target *actor.PID, gvr schema.GroupVersionResource) (KubernetesResourceStore[unstructured.Unstructured], bool) {
	return dy.gvrMap.Get(formGVRKey(target, gvr))
}
