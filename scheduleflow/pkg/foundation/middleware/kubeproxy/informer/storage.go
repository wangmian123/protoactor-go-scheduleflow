package informer

import (
	"fmt"

	jsoniter "github.com/json-iterator/go"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer/fundamental"
	cmap "github.com/orcaman/concurrent-map"
)

var json jsoniter.API

func init() {
	json = jsoniter.ConfigCompatibleWithStandardLibrary
}

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
