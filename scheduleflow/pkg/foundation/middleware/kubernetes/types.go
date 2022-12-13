package kubernetes

import (
	"context"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

const (
	OperatorDefaultTimeOut = 600 * time.Second
	DefaultQPS             = 100
	DefaultBurst           = 500
	DefaultCreationPool    = 200
	DefaultUpdatingPool    = 200
)

type operatorConfig struct {
	timeout *time.Duration
}

type middlewareConfig struct {
	ctx     actor.Context
	tagName string
}

type serverConfig struct {
	creationPoolSize int
	updatingPoolSize int
	clientQPS        float32
	burst            int
}

type config struct {
	operatorConfig
	middlewareConfig
	serverConfig
}

func newDefaultConfig() *config {
	timeout := OperatorDefaultTimeOut
	return &config{
		operatorConfig: operatorConfig{
			timeout: &timeout,
		},
		middlewareConfig: middlewareConfig{},
		serverConfig: serverConfig{
			creationPoolSize: DefaultCreationPool,
			updatingPoolSize: DefaultUpdatingPool,
			clientQPS:        DefaultQPS,
			burst:            DefaultBurst,
		},
	}
}

type Option func(*config)

func WithKubernetesCreationLimits(size int) Option {
	return func(c *config) {
		c.creationPoolSize = size
	}
}

func WithKubernetesUpdatingLimits(size int) Option {
	return func(c *config) {
		c.updatingPoolSize = size
	}
}

func WithDefaultTimeout(duration time.Duration) Option {
	return func(c *config) {
		c.timeout = &duration
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

func WithKubernetesClientQPS(qps float32) Option {
	return func(c *config) {
		c.clientQPS = qps
	}
}

func WithKubernetesClientBurst(burst int) Option {
	return func(c *config) {
		c.burst = burst
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

type NamespaceableResourceInterface[R any] interface {
	Namespace(string) ResourceOperator[R]
	ResourceOperator[R]
}

type DynamicNamespaceableOperator = NamespaceableResourceInterface[unstructured.Unstructured]
type DynamicOperator = ResourceOperator[unstructured.Unstructured]

type ResourceOperator[R any] interface {
	SynchronizeResource[R]
	Create(ctx context.Context, obj *R, options metav1.CreateOptions, subresources ...string) (*R, error)
	Update(ctx context.Context, obj *R, options metav1.UpdateOptions, subresources ...string) (*R, error)
	UpdateStatus(ctx context.Context, obj *R, options metav1.UpdateOptions) (*R, error)
	Delete(ctx context.Context, name string, options metav1.DeleteOptions, subresources ...string) error
	Get(ctx context.Context, name string, options metav1.GetOptions, subresources ...string) (*R, error)
	List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error)
	ListSlice(ctx context.Context, opts metav1.ListOptions) ([]*R, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, options metav1.PatchOptions, subresources ...string) (*R, error)
	PatchStatus(ctx context.Context, name string, pt types.PatchType, data []byte, options metav1.PatchOptions) (*R, error)
}

type SynchronizeResource[R any] interface {
	BlockGet(ctx context.Context, name string, options kubeproxy.BlockGetOptions, subresources ...string) (*R, error)
	UnlockResource(ctx context.Context, name string) error
	Synchronize(ctx context.Context, original, synchronizing *R, options metav1.UpdateOptions) (*R, error)
}
