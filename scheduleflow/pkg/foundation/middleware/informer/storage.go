package informer

import (
	"context"
	"fmt"
	"time"

	"github.com/LiuYuuChen/algorithms/queue"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer/fundamental"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"
	jsoniter "github.com/json-iterator/go"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var json jsoniter.API

func init() {
	json = jsoniter.ConfigCompatibleWithStandardLibrary
}

//k8sOperableResourceStore
type k8sOperableResourceStore[R any] struct {
	storeConstraint  StoreConstraint[R]
	resourceOnComing queue.DelayingQueue[*ResourceChanging[R]]

	handlers []fundamental.ResourceEventHandlerFuncs[R]
	store    cmap.ConcurrentMap[R]

	updateInterval time.Duration
}

type dynamicStore = k8sOperableResourceStore[unstructured.Unstructured]

func newOperableResourceStore[R any](formKey StoreConstraint[R], interval time.Duration) *k8sOperableResourceStore[R] {
	store := &k8sOperableResourceStore[R]{
		storeConstraint:  formKey,
		store:            cmap.New[R](),
		resourceOnComing: queue.NewDelayingQueue[*ResourceChanging[R]](newChangeConstraint(formKey)),
		updateInterval:   interval,
	}

	go store.informResourceChange(context.Background())

	return store
}

func (k *k8sOperableResourceStore[R]) AddFunc(resource R) {
	key := k.storeConstraint.FormKey(resource)
	_, ok := k.store.Get(key)
	if ok {
		return
	}
	k.store.Set(key, resource)
	for _, handler := range k.handlers {
		handler.AddFunc(k.storeConstraint.DeepCopy(resource))
	}
}

func (k *k8sOperableResourceStore[R]) DeleteFunc(resource R) {
	key := k.storeConstraint.FormKey(resource)
	change := &ResourceChanging[R]{
		NewResource: &resource,
	}
	_ = k.resourceOnComing.Delete(change)

	for _, handler := range k.handlers {
		handler.DeleteFunc(k.storeConstraint.DeepCopy(resource))
	}

	_, ok := k.store.Get(key)
	if !ok {
		return
	}
	k.store.Remove(key)
}

func (k *k8sOperableResourceStore[R]) UpdateFunc(oldResource, newResource R) {
	key := k.storeConstraint.FormKey(newResource)
	k.store.Set(key, newResource)
	change := &ResourceChanging[R]{
		NewResource: &newResource,
		OldResource: &oldResource,
	}
	_, ok := k.resourceOnComing.Get(change)
	if !ok {
		k.resourceOnComing.AddAfter(change, k.updateInterval)
		return
	}

	err := k.resourceOnComing.Refresh(change)
	if err != nil {
		logrus.Errorf("refresh resource with err %v", err)
	}
	return
}

func (k *k8sOperableResourceStore[R]) Unmarshal(data []byte) (R, error) {
	for _, handler := range k.handlers {
		if handler.Unmarshal != nil {
			return handler.Unmarshal(data)
		}
	}
	var res R
	err := json.Unmarshal(data, &res)
	if err != nil {
		return res, fmt.Errorf("unmarshal gvr error with %v", err)
	}
	return res, nil
}

func (k *k8sOperableResourceStore[R]) SetResourceHandler(handler fundamental.ResourceEventHandlerFuncs[R]) {
	k.handlers = append(k.handlers, handler)
	if k.store.IsEmpty() {
		return
	}
	go func() {
		for _, resource := range k.store.Items() {
			for _, h := range k.handlers {
				h.AddFunc(k.storeConstraint.DeepCopy(resource))
			}
		}
	}()
}

func (k *k8sOperableResourceStore[R]) Get(resource R) (R, bool) {
	key := k.storeConstraint.FormKey(resource)
	raw, ok := k.store.Get(key)
	if !ok {
		return *new(R), false
	}
	return raw, true
}

func (k *k8sOperableResourceStore[R]) List() []R {
	var list []R
	for _, value := range k.store.Items() {
		list = append(list, value)
	}
	return list
}

func (k *k8sOperableResourceStore[R]) Set(resource R) {
	key := k.storeConstraint.FormKey(resource)
	k.store.Set(key, resource)
}

func (k *k8sOperableResourceStore[R]) informResourceChange(ctx context.Context) {
	for {
		select {
		case _, ok := <-ctx.Done():
			if !ok {
				logrus.Errorf("%sunexpected closed", logPrefix)
			}
			k.resourceOnComing.Shutdown()
			return
		default:
		}

		item, err := k.resourceOnComing.Pop()
		if err != nil {
			logrus.Errorf("%s informer store pop error due to %v", logPrefix, err)
			continue
		}

		for _, handler := range k.handlers {
			oldResource, newResource := item.OldResource, item.NewResource
			handler.UpdateFunc(k.storeConstraint.DeepCopy(*oldResource), k.storeConstraint.DeepCopy(*newResource))
		}
	}
}

type storeConvertor[I, O any] struct {
	store KubernetesResourceStore[I]
}

func NewConvertedStore[I, O any](store KubernetesResourceStore[I]) KubernetesResourceStore[O] {
	return &storeConvertor[I, O]{
		store: store,
	}
}

func (s *storeConvertor[I, O]) Get(resource O) (O, bool) {
	converted, err := utils.ResourceConvert[O, I](&resource)
	if err != nil {
		logrus.Errorf("can not convert resource due to %v", err)
		return *new(O), false
	}
	storedRes, ok := s.store.Get(*converted)
	if !ok {
		return *new(O), false
	}
	storedResConverted, err := utils.ResourceConvert[I, O](&storedRes)
	if err != nil {
		logrus.Errorf("can not convert resource due to %v", err)
		return *new(O), false
	}
	return *storedResConverted, true
}

func (s *storeConvertor[I, O]) List() []O {
	storedList := s.store.List()
	list := make([]O, 0, len(storedList))
	for _, storedRes := range storedList {
		res, err := utils.ResourceConvert[I, O](&storedRes)
		if err != nil {
			logrus.Errorf("can not convert resource due to %v", err)
		}
		list = append(list, *res)
	}
	return list
}
