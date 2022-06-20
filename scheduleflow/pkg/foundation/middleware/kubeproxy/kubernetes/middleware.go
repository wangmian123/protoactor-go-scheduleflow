package kubernetes

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type builder struct {
	ctx  actor.Context
	apis cmap.ConcurrentMap[ResourceInterface]
}

func NewKubernetesAPIBuilder(ctx actor.Context) Builder {
	return &builder{
		ctx:  ctx,
		apis: cmap.New[ResourceInterface](),
	}
}

func (b *builder) GetResourceInterface(target *actor.PID) ResourceInterface {
	if target == nil {
		return nil
	}

	api, ok := b.apis.Get(utils.FormActorKey(target))
	if !ok {
		api = NewKubernetesAPI(target, b.ctx)
		b.apis.Set(utils.FormActorKey(target), api)
	}

	return api
}

func (b *builder) ListResourceInterface() map[string]ResourceInterface {
	return b.apis.Items()
}

type kubernetesAPI struct {
	ctx       actor.Context
	resource  *schema.GroupVersionResource
	namespace string
	target    *actor.PID
}

func NewKubernetesAPI(target *actor.PID, ctx actor.Context, opts ...Option,
) ResourceInterface {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	return &kubernetesAPI{
		target: target,
		ctx:    ctx,
	}
}

func (k *kubernetesAPI) Resource(resource schema.GroupVersionResource,
) NamespaceableResourceInterface[unstructured.Unstructured] {
	k.resource = &resource
	operator := NewOperator[unstructured.Unstructured](k.ctx, k.target, &resource)
	return operator
}

func (k *kubernetesAPI) CoreV1() CoreV1Interface {
	return k
}

func (k *kubernetesAPI) Pods() NamespaceableResourceInterface[v1.Pod] {
	gvr := schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "pods",
	}

	operator := NewOperator[v1.Pod](k.ctx, k.target, &gvr)
	return operator
}

func (k *kubernetesAPI) Nodes() NamespaceableResourceInterface[v1.Node] {
	gvr := schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "nodes",
	}

	operator := NewOperator[v1.Node](k.ctx, k.target, &gvr)
	return operator
}

func BuilderMiddlewareProducer(opts ...Option) actor.ReceiverMiddleware {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	started := func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) bool {
		ctx := c.(actor.Context)
		if cfg.ctx != nil {
			ctx = cfg.ctx
		}
		api := NewKubernetesAPIBuilder(ctx)
		err := utils.InjectActor(c, utils.NewInjectorItem(cfg.tagName, api))
		if err != nil {
			logrus.Error(err)
		}
		return false
	}

	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(started).
		ProduceReceiverMiddleware()
}

func MiddlewareProducer(target *actor.PID, opts ...Option) actor.ReceiverMiddleware {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	started := func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) bool {
		ctx := c.(actor.Context)
		api := NewKubernetesAPI(target, ctx)
		err := utils.InjectActor(c, utils.NewInjectorItem(cfg.tagName, api))
		if err != nil {
			logrus.Error(err)
		}
		return false
	}

	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(started).ProduceReceiverMiddleware()
}
