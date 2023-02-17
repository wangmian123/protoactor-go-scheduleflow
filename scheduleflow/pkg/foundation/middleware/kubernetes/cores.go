package kubernetes

import (
	"context"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/informer"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"
	cmap "github.com/orcaman/concurrent-map"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

type builder struct {
	ctx  actor.Context
	apis cmap.ConcurrentMap[ResourceInterface]
	opts []Option
}

func NewKubernetesAPIBuilder(ctx actor.Context, opts ...Option) Builder {
	return &builder{
		ctx:  ctx,
		apis: cmap.New[ResourceInterface](),
		opts: opts,
	}
}

func (b *builder) GetResourceInterface(target *actor.PID) ResourceInterface {
	if target == nil {
		return nil
	}

	target = GetAPI(target.Address)

	api, ok := b.apis.Get(utils.FormActorKey(target))
	if !ok {
		api = NewKubernetesAPI(target, b.ctx, b.opts...)
		b.apis.Set(utils.FormActorKey(target), api)
	}

	return api
}

func (b *builder) ListResourceInterface() map[string]ResourceInterface {
	return b.apis.Items()
}

type kubernetesAPI struct {
	ctx    actor.Context
	target *actor.PID

	opts []Option
}

func NewKubernetesAPI(target *actor.PID, ctx actor.Context, opts ...Option) ResourceInterface {
	return &kubernetesAPI{
		target: target,
		ctx:    ctx,
		opts:   opts,
	}
}

func (k *kubernetesAPI) Resource(resource schema.GroupVersionResource) NamespaceableResourceInterface[unstructured.Unstructured] {
	operator := NewOperator[unstructured.Unstructured](k.ctx, k.target, &resource, k.opts...)
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

func ConvertDynamicTo[R any](dyAPI NamespaceableResourceInterface[unstructured.Unstructured]) NamespaceableResourceInterface[R] {
	return &dynamicConvertor[R]{dyAPI: dyAPI}
}

type dynamicConvertor[R any] struct {
	dyAPI NamespaceableResourceInterface[unstructured.Unstructured]
}

func (cvt *dynamicConvertor[R]) Namespace(namespace string) ResourceOperator[R] {
	cvt.dyAPI.Namespace(namespace)
	return cvt
}

func (cvt *dynamicConvertor[R]) Create(ctx context.Context, obj *R, options metav1.CreateOptions, subresources ...string) (*R, error) {
	res, err := informer.ConvertToUnstructured[R](obj)
	if err != nil {
		return nil, err
	}

	updated, err := cvt.dyAPI.Create(ctx, res, options, subresources...)
	if err != nil {
		return nil, err
	}

	updatedObj, err := informer.ConvertToResource[R](updated)
	if err != nil {
		return nil, err
	}
	return updatedObj, nil
}

func (cvt *dynamicConvertor[R]) Update(ctx context.Context, obj *R, options metav1.UpdateOptions, subresources ...string) (*R, error) {
	res, err := informer.ConvertToUnstructured[R](obj)
	if err != nil {
		return nil, err
	}

	updated, err := cvt.dyAPI.Update(ctx, res, options, subresources...)
	if err != nil {
		return nil, err
	}

	updatedObj, err := informer.ConvertToResource[R](updated)
	if err != nil {
		return nil, err
	}
	return updatedObj, nil
}

func (cvt *dynamicConvertor[R]) UpdateStatus(ctx context.Context, obj *R, options metav1.UpdateOptions) (*R, error) {
	res, err := informer.ConvertToUnstructured[R](obj)
	if err != nil {
		return nil, err
	}

	updated, err := cvt.dyAPI.UpdateStatus(ctx, res, options)
	if err != nil {
		return nil, err
	}

	updatedObj, err := informer.ConvertToResource[R](updated)
	if err != nil {
		return nil, err
	}
	return updatedObj, nil
}

func (cvt *dynamicConvertor[R]) Delete(ctx context.Context, name string, options metav1.DeleteOptions, subresources ...string) error {
	return cvt.dyAPI.Delete(ctx, name, options, subresources...)
}

func (cvt *dynamicConvertor[R]) Get(ctx context.Context, name string, options metav1.GetOptions, subresources ...string) (*R, error) {
	updated, err := cvt.dyAPI.Get(ctx, name, options, subresources...)
	if err != nil {
		return nil, err
	}

	updatedObj, err := informer.ConvertToResource[R](updated)
	if err != nil {
		return nil, err
	}
	return updatedObj, nil
}

func (cvt *dynamicConvertor[R]) List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	return cvt.dyAPI.List(ctx, opts)
}

func (cvt *dynamicConvertor[R]) ListSlice(ctx context.Context, opts metav1.ListOptions) ([]*R, error) {
	resList, err := cvt.dyAPI.ListSlice(ctx, opts)
	if err != nil {
		return nil, err
	}

	convertedList := make([]*R, 0, len(resList))
	for _, res := range resList {
		updatedObj, err := informer.ConvertToResource[R](res)
		if err != nil {
			return nil, err
		}
		convertedList = append(convertedList, updatedObj)
	}
	return convertedList, nil
}

func (cvt *dynamicConvertor[R]) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, options metav1.PatchOptions, subresources ...string) (*R, error) {
	patched, err := cvt.dyAPI.Patch(ctx, name, pt, data, options, subresources...)
	if err != nil {
		return nil, err
	}

	updatedObj, err := informer.ConvertToResource[R](patched)
	if err != nil {
		return nil, err
	}
	return updatedObj, nil
}

func (cvt *dynamicConvertor[R]) BlockGet(ctx context.Context, name string, options kubeproxy.BlockGetOptions, subresources ...string) (*R, error) {
	res, err := cvt.dyAPI.BlockGet(ctx, name, options, subresources...)
	if err != nil {
		return nil, err
	}

	updatedObj, err := informer.ConvertToResource[R](res)
	if err != nil {
		return nil, err
	}
	return updatedObj, nil
}

func (cvt *dynamicConvertor[R]) UnlockResource(ctx context.Context, name string) error {
	return cvt.dyAPI.UnlockResource(ctx, name)
}

func (cvt *dynamicConvertor[R]) PatchStatus(ctx context.Context, name string, pt types.PatchType, data []byte, options metav1.PatchOptions) (*R, error) {
	patched, err := cvt.dyAPI.PatchStatus(ctx, name, pt, data, options)
	if err != nil {
		return nil, err
	}

	updatedObj, err := informer.ConvertToResource[R](patched)
	if err != nil {
		return nil, err
	}
	return updatedObj, nil
}

func (cvt *dynamicConvertor[R]) Synchronize(ctx context.Context, original, synchronizing *R, options metav1.UpdateOptions) (*R, error) {
	dyOriginal, err := informer.ConvertToUnstructured[R](original)
	if err != nil {
		return nil, err
	}

	dySynchronizing, err := informer.ConvertToUnstructured[R](synchronizing)
	if err != nil {
		return nil, err
	}

	dySynchronized, err := cvt.dyAPI.Synchronize(ctx, dyOriginal, dySynchronizing, options)
	if err != nil {
		return nil, err
	}

	synchronized, err := informer.ConvertToResource[R](dySynchronized)
	if err != nil {
		return nil, err
	}

	return synchronized, nil
}
