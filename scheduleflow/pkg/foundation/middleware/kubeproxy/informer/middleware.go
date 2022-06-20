package informer

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer/fundamental"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
)

const (
	logPrefix = "[ClusterInformerProxy]"
	timeout   = 30 * time.Second
)

//k8sResourceSubscriberWithStore
type k8sResourceSubscriberWithStore[R any] struct {
	storeConstraint StoreConstraint[R]
	handler         fundamental.ResourceEventHandlerFuncs[R]
	store           cmap.ConcurrentMap[R]
}

func newK8sResourceSubscribeWithStore[R any](formKey StoreConstraint[R]) *k8sResourceSubscriberWithStore[R] {
	return &k8sResourceSubscriberWithStore[R]{
		storeConstraint: formKey,
		store:           cmap.New[R](),
	}
}

func (k *k8sResourceSubscriberWithStore[R]) AddFunc(resource R) {
	key := k.storeConstraint.FormKey(resource)
	k.store.Set(key, resource)
	if k.handler != nil {
		k.handler.AddFunc(k.storeConstraint.DeepCopy(resource))
	}
}

func (k *k8sResourceSubscriberWithStore[R]) DeleteFunc(resource R) {
	key := k.storeConstraint.FormKey(resource)
	if k.handler != nil {
		k.handler.DeleteFunc(k.storeConstraint.DeepCopy(resource))
	}

	_, ok := k.store.Get(key)
	if !ok {
		return
	}
	k.store.Remove(key)
}

func (k *k8sResourceSubscriberWithStore[R]) UpdateFunc(oldResource, newResource R) {
	key := k.storeConstraint.FormKey(newResource)
	k.store.Set(key, newResource)
	if k.handler != nil {
		k.handler.UpdateFunc(k.storeConstraint.DeepCopy(oldResource), k.storeConstraint.DeepCopy(newResource))
	}
}

func (k *k8sResourceSubscriberWithStore[R]) Unmarshal(data []byte) (R, error) {
	if k.handler.Unmarshal != nil {
		return k.handler.Unmarshal(data)
	}
	var res R
	err := json.Unmarshal(data, &res)
	if err != nil {
		return res, fmt.Errorf("unmarshal gvr error with %v", err)
	}
	return res, nil
}

func (k *k8sResourceSubscriberWithStore[R]) SetResourceHandler(handler fundamental.ResourceEventHandlerFuncs[R]) {
	k.handler = handler
	if k.store.IsEmpty() {
		return
	}
	go func() {
		for _, res := range k.store.Items() {
			k.handler.AddFunc(k.storeConstraint.DeepCopy(res))
		}
	}()
}

func (k *k8sResourceSubscriberWithStore[R]) Get(resource R) (R, bool) {
	key := k.storeConstraint.FormKey(resource)
	raw, ok := k.store.Get(key)
	if !ok {
		return *new(R), false
	}
	return raw, true
}

func (k *k8sResourceSubscriberWithStore[R]) List() []R {
	var list []R
	for _, value := range k.store.Items() {
		list = append(list, value)
	}
	return list
}

// middlewareProducer
type middlewareProducer[R any] struct {
	store     *k8sResourceSubscriberWithStore[R]
	injectTag string
}

func MiddlewareProducer[R any](constraint StoreConstraint[R], tagName string) *middlewareProducer[R] {
	store := newK8sResourceSubscribeWithStore(constraint)
	return &middlewareProducer[R]{
		store:     store,
		injectTag: tagName,
	}
}

func (mid *middlewareProducer[R]) ProduceSubscribeMiddleware(target *actor.PID, subRes SubscribeResource,
	handler fundamental.ResourceEventHandlerFuncs[R]) actor.ReceiverMiddleware {
	mid.store.handler = handler
	subscribeEvent := fundamental.SubscribeResourceFrom[R]{
		Target: target,
		Resource: kubeproxy.SubscribeResource{
			GVR:        kubeproxy.NewGroupVersionResource(subRes.GVR),
			ActionCode: 0,
			Option:     &kubeproxy.SubscribeOption{RateLimitation: subRes.Option.RateLimitation},
		},
		Handler: mid.store,
	}

	started := func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) bool {
		err := mid.onStart(c, subscribeEvent)
		if err != nil {
			logrus.Error(err)
		}
		return false
	}

	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(started).ProduceReceiverMiddleware()
}

func (mid *middlewareProducer[R]) onStart(c actor.ReceiverContext, subscribeEvent fundamental.SubscribeResourceFrom[R]) error {
	err := utils.InjectActor(c, utils.NewInjectorItem(mid.injectTag, mid.store))
	if err != nil {
		return err
	}

	ctx := c.(actor.Context)

	informerPid := actor.NewPID(c.Self().Address, c.Self().Id+"/InformerProxy")
	_, ok := c.ActorSystem().ProcessRegistry.Get(informerPid)

	if !ok {
		var err error
		informerPid, err = ctx.SpawnNamed(informer.New(), "InformerProxy")
		if err != nil {
			return fmt.Errorf("spawn informer-server proxy fail due to %v", err)
		}
	}

	future := ctx.RequestFuture(informerPid, &subscribeEvent, timeout)
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
	return nil
}
