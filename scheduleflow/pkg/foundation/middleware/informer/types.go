package informer

import (
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer/fundamental"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type (
	UnstructuredOperableStore = KubernetesOperableResourceStore[unstructured.Unstructured]
	UnstructuredStore         = KubernetesResourceStore[unstructured.Unstructured]
)

// DynamicEventHandler recalls when resource add, delete or update.
type DynamicEventHandler interface {
	AddFunc(resource unstructured.Unstructured)
	DeleteFunc(resource unstructured.Unstructured)
	UpdateFunc(oldResource, newResource unstructured.Unstructured)
}

// DynamicInformer set or get resource informer.
type DynamicInformer interface {
	SetResourceHandler(subscribe ...DynamicSubscribe) ([]KubernetesOperableResourceStore[unstructured.Unstructured], error)
	GetResourceStore(pid *actor.PID, gvr schema.GroupVersionResource) (KubernetesResourceStore[unstructured.Unstructured], bool)
}

// EventHandler recalls when resource add, delete or update.
type EventHandler[R any] interface {
	AddFunc(resource R)
	DeleteFunc(resource R)
	UpdateFunc(oldResource, newResource R)
}

type Informer[R any] interface {
	SetResourceHandler(subscribe ...Subscribe[R]) ([]KubernetesOperableResourceStore[R], error)
	GetResourceStore(pid *actor.PID, gvr schema.GroupVersionResource) (KubernetesResourceStore[R], bool)
}

type Subscribe[R any] struct {
	Target          *actor.PID
	Resource        SubscribeResource
	StoreConstraint StoreConstraint[R]
	Handler         EventHandler[R]
	UpdateInterval  time.Duration
}

type KubernetesOperableResourceStore[R any] interface {
	Set(R)
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

func WithUpdateInterval(interval time.Duration) Option {
	return func(producer *config) {
		producer.updateInterval = interval
	}
}

type config struct {
	SubscribeOption
	storeConfig
	serverConfig
	tagName string
}

func newConfig() *config {
	return &config{
		serverConfig: serverConfig{
			qps:   DefaultQPS,
			burst: DefaultBurst,
		},
		tagName: "",
	}
}

type serverConfig struct {
	qps   float32
	burst int
}

type storeConfig struct {
	handler         DynamicEventHandler
	storeConstraint StoreConstraint[unstructured.Unstructured]

	updateInterval time.Duration
}

// SubscribeOption subscription options
type SubscribeOption struct {
	RateLimitation int64
}

func convertSubscribeOption(opt SubscribeOption) *kubeproxy.SubscribeOption {
	return &kubeproxy.SubscribeOption{
		RateLimitation: opt.RateLimitation,
	}
}

// SubscribeResource defines subscribe resource.
type SubscribeResource struct {
	GVR    schema.GroupVersionResource
	Option SubscribeOption
}

func convertSubscribeResource(sub SubscribeResource) *kubeproxy.SubscribeResource {
	return &kubeproxy.SubscribeResource{
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

// podStoreConstraint
type podStoreConstraint struct {
}

func (*podStoreConstraint) FormKey(pod v1.Pod) string {
	return FormPodKey(pod)
}

func (*podStoreConstraint) DeepCopy(pod v1.Pod) v1.Pod {
	return *DeepCopy[*v1.Pod](&pod)
}

// nodeStoreConstraint
type nodeStoreConstraint struct {
}

func (*nodeStoreConstraint) FormKey(node v1.Node) string {
	return FormNodeKey(node)
}

func (*nodeStoreConstraint) DeepCopy(node v1.Node) v1.Node {
	return *DeepCopy[*v1.Node](&node)
}

// defaultConstraint
type defaultConstraint struct {
}

func (d *defaultConstraint) FormKey(res unstructured.Unstructured) string {
	return res.GetNamespace() + "/" + res.GetName()
}

func (d *defaultConstraint) DeepCopy(res unstructured.Unstructured) unstructured.Unstructured {
	return *res.DeepCopy()
}

type ResourceChanging[R any] struct {
	NewResource *R
	OldResource *R
}

type resourceChangingStoreConstraint[R any] struct {
	constraint StoreConstraint[R]
}

func newChangeConstraint[R any](constraint StoreConstraint[R]) *resourceChangingStoreConstraint[R] {
	return &resourceChangingStoreConstraint[R]{
		constraint: constraint,
	}
}

func (c *resourceChangingStoreConstraint[R]) FormStoreKey(res *ResourceChanging[R]) string {
	if res.NewResource == nil {
		return ""
	}
	return c.constraint.FormKey(*res.NewResource)
}

func (c *resourceChangingStoreConstraint[R]) Less(*ResourceChanging[R], *ResourceChanging[R]) bool {
	return false
}

type HandlerFunc = EventHandlerFunc[unstructured.Unstructured]

type EventHandlerFunc[R any] struct {
	Creating func(resource R)
	Deleting func(resource R)
	Updating func(oldResource, newResource R)
}

func (h *EventHandlerFunc[R]) AddFunc(resource R) {
	if h.Creating != nil {
		h.Creating(resource)
	}
}

func (h *EventHandlerFunc[R]) DeleteFunc(resource R) {
	if h.Deleting != nil {
		h.Deleting(resource)
	}
}

func (h *EventHandlerFunc[R]) UpdateFunc(oldResource, newResource R) {
	if h.Updating != nil {
		h.Updating(oldResource, newResource)
	}
}
