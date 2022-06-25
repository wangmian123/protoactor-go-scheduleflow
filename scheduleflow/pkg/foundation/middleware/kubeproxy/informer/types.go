package informer

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer/fundamental"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var _ fundamental.ResourceEventHandlerFuncs[v1.Pod] = &k8sResourceSubscriberWithStore[v1.Pod]{}

// DynamicEventHandler recalls when resource add, delete or update.
type DynamicEventHandler interface {
	AddFunc(resource unstructured.Unstructured)
	DeleteFunc(resource unstructured.Unstructured)
	UpdateFunc(oldResource, newResource unstructured.Unstructured)
}

// DynamicInformer set or get resource informer.
type DynamicInformer interface {
	SetResourceHandler(ctx actor.Context, subscribe ...DynamicSubscribe) error
	GetResourceStore(pid *actor.PID, gvr schema.GroupVersionResource) (KubernetesResourceStore[unstructured.Unstructured], bool)
}

type KubernetesResourceSubscriber[R any] interface {
	SetResourceHandler(handler fundamental.ResourceEventHandlerFuncs[R])
	KubernetesResourceStore[R]
}

type KubernetesResourceStore[R any] interface {
	Get(resource R) (R, bool)
	List() []R
}

type KubernetesResourceHandler[R any] interface {
	SetResourceHandler(handler fundamental.ResourceEventHandlerFuncs[R])
	SynchronizeStore(groundTruth []R)
}

type StoreConstraint[R any] interface {
	FormKey(R) string
	DeepCopy(R) R
}

type DeepCopier[R any] interface {
	DeepCopy() R
}

type Option func(*config)

func WithTagName(name string) Option {
	return func(producer *config) {
		producer.tagName = name
	}
}

func WithRateLimitation(rate int64) Option {
	return func(producer *config) {
		producer.RateLimitation = rate
	}
}

type config struct {
	SubscribeOption
	storeConfig
	tagName string
}

type storeConfig struct {
	handler         DynamicEventHandler
	storeConstraint StoreConstraint[unstructured.Unstructured]
}

// SubscribeOption
type SubscribeOption struct {
	RateLimitation int64
}

func convertSubscribeOption(opt SubscribeOption) *kubeproxy.SubscribeOption {
	return &kubeproxy.SubscribeOption{
		RateLimitation: opt.RateLimitation,
	}
}

//SubscribeResource
type SubscribeResource struct {
	GVR    schema.GroupVersionResource
	Option SubscribeOption
}

func convertSubscribeResource(sub SubscribeResource) kubeproxy.SubscribeResource {
	return kubeproxy.SubscribeResource{
		GVR:        kubeproxy.NewGroupVersionResource(sub.GVR),
		ActionCode: 0,
		Option:     convertSubscribeOption(sub.Option),
	}
}

func DeepCopy[R any](source DeepCopier[R]) R {
	return source.DeepCopy()
}

func FormResourceKey(obj metav1.ObjectMeta) string {
	return obj.Namespace + "/" + obj.Namespace
}

//podStoreConstraint
type podStoreConstraint struct {
}

func (*podStoreConstraint) FormKey(pod v1.Pod) string {
	return FormPodKey(pod)
}

func (*podStoreConstraint) DeepCopy(pod v1.Pod) v1.Pod {
	return *DeepCopy[*v1.Pod](&pod)
}

//nodeStoreConstraint
type nodeStoreConstraint struct {
}

func (*nodeStoreConstraint) FormKey(node v1.Node) string {
	return FormNodeKey(node)
}

func (*nodeStoreConstraint) DeepCopy(node v1.Node) v1.Node {
	return *DeepCopy[*v1.Node](&node)
}

type defaultConstraint struct {
}

func (d *defaultConstraint) FormKey(res unstructured.Unstructured) string {
	return res.GetNamespace() + "/" + res.GetName()
}

func (d *defaultConstraint) DeepCopy(res unstructured.Unstructured) unstructured.Unstructured {
	return *res.DeepCopy()
}
