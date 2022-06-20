package informer

import (
	"encoding/json"
	"fmt"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer/fundamental"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const informerName = "InformerProxy"

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
		informerPid, err := ctx.SpawnNamed(informer.New(), informerName)
		if err != nil {
			return fmt.Errorf("spawn informer-server proxy fail due to %v", err)
		}
		dy.informerPid = informerPid
	}

	for _, subInfo := range subscribe {
		resourceStore := newK8sResourceSubscribeWithStore[unstructured.Unstructured](subInfo.StoreConstraint)
		resourceStore.handler = convertDynamicEventHandler(subInfo.Handler)

		subscribeEvent := fundamental.SubscribeResourceFrom[unstructured.Unstructured]{
			Target:   subInfo.Target,
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

func NewDynamicSubscribeProducer(opts ...Option) actor.ReceiverMiddleware {
	producer := &config{}
	for _, opt := range opts {
		opt(producer)
	}

	started := func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) bool {
		informerPid := actor.NewPID(c.Self().Address, c.Self().Id+"/"+informerName)
		_, ok := c.ActorSystem().ProcessRegistry.Get(informerPid)
		ctx := c.(actor.Context)

		if !ok {
			var err error
			informerPid, err = ctx.SpawnNamed(informer.New(), informerName)
			if err != nil {
				logrus.Errorf("spawn informer-server proxy fail due to %v", err)
				return false
			}
		}

		err := utils.InjectActor(c, utils.NewInjectorItem(producer.tagName, newDynamicInformer(informerPid)))
		if err != nil {
			logrus.Error(err)
		}
		return false
	}
	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(started).ProduceReceiverMiddleware()
}
