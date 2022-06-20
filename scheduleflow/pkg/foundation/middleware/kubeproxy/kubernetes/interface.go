package kubernetes

import (
	"context"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

const timeout = 60 * time.Second

type operatorConfig struct {
	timeout time.Duration
}

type middlewareConfig struct {
	ctx     actor.Context
	tagName string
}

type config struct {
	operatorConfig
	middlewareConfig
}

type Option func(*config)

func WithDefaultTimeout(duration time.Duration) Option {
	return func(c *config) {
		c.timeout = duration
	}
}

func WithContextAppointment(ctx actor.Context) Option {
	return func(c *config) {
		c.ctx = ctx
	}
}

func WithTagName(name string) Option {
	return func(c *config) {
		c.tagName = name
	}
}

type Builder interface {
	GetResourceInterface(target *actor.PID) ResourceInterface
	ListResourceInterface() map[string]ResourceInterface
}

type ResourceInterface interface {
	CoreInterface
	DynamicResourceInterface
}

type DynamicResourceInterface interface {
	Resource(resource schema.GroupVersionResource) NamespaceableResourceInterface[unstructured.Unstructured]
}

type CoreInterface interface {
	CoreV1() CoreV1Interface
}

type CoreV1Interface interface {
	PodsGetter
	NodesGetter
}

type PodsGetter interface {
	Pods() NamespaceableResourceInterface[v1.Pod]
}

type NodesGetter interface {
	Nodes() NamespaceableResourceInterface[v1.Node]
}

//type EventsGetter interface{
//	Events()NamespaceableResourceInterface[v1.Event]
//}

type NamespaceableResourceInterface[R any] interface {
	Namespace(string) ResourceOperator[R]
	ResourceOperator[R]
}

type ResourceOperator[R any] interface {
	Create(ctx context.Context, obj *R, options metav1.CreateOptions, subresources ...string) (*R, error)
	Update(ctx context.Context, obj *R, options metav1.UpdateOptions, subresources ...string) (*R, error)
	UpdateStatus(ctx context.Context, obj *R, options metav1.UpdateOptions) (*R, error)
	Delete(ctx context.Context, name string, options metav1.DeleteOptions, subresources ...string) error
	Get(ctx context.Context, name string, options metav1.GetOptions, subresources ...string) (*R, error)
	List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error)
	ListSlice(ctx context.Context, opts metav1.ListOptions) ([]R, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, options metav1.PatchOptions, subresources ...string) (*R, error)
}
