package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
)

type OperateResource[R any] struct {
	ctx       actor.Context
	target    *actor.PID
	gvr       *schema.GroupVersionResource
	namespace string
	timeout   time.Duration
}

func NewOperator[R any](ctx actor.Context, target *actor.PID, gvr *schema.GroupVersionResource,
	opts ...Option) *OperateResource[R] {
	cfg := config{}
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.timeout == nil {
		timeout := OperatorDefaultTimeOut
		cfg.timeout = &timeout
	}

	return &OperateResource[R]{
		ctx:       ctx,
		target:    target,
		gvr:       gvr,
		namespace: metav1.NamespaceAll,
		timeout:   *cfg.timeout,
	}
}

func (r *OperateResource[R]) Create(ctx context.Context, obj *R, options metav1.CreateOptions,
	subresources ...string) (*R, error) {
	rawResource, err := marshal[R](obj)
	if err != nil {
		return nil, err
	}

	createInfo := &kubeproxy.Create{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
		},
		GVR:           kubeproxy.NewGroupVersionResource(*r.gvr),
		CreateOptions: &options,
		Resource:      rawResource,
		SubResources:  subresources,
	}

	return r.requestUntilTimeout(ctx, createInfo)
}

func (r *OperateResource[R]) Update(ctx context.Context, obj *R, options metav1.UpdateOptions,
	subresources ...string) (*R, error) {
	rawResource, err := marshal[R](obj)
	if err != nil {
		return nil, err
	}

	updateInfo := &kubeproxy.Update{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
		},
		GVR:           kubeproxy.NewGroupVersionResource(*r.gvr),
		UpdateOptions: &options,
		Resource:      rawResource,
		SubResources:  subresources,
	}

	return r.requestUntilTimeout(ctx, updateInfo)
}

func (r *OperateResource[R]) UpdateStatus(ctx context.Context, obj *R, options metav1.UpdateOptions) (*R, error) {
	rawResource, err := marshal[R](obj)
	if err != nil {
		return nil, err
	}

	updateInfo := &kubeproxy.UpdateStatus{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
		},
		GVR:           kubeproxy.NewGroupVersionResource(*r.gvr),
		UpdateOptions: &options,
		Resource:      rawResource,
	}

	return r.requestUntilTimeout(ctx, updateInfo)
}

func (r *OperateResource[R]) Delete(ctx context.Context, name string, options metav1.DeleteOptions,
	subresources ...string) error {
	deleteInfo := &kubeproxy.Delete{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
			Name:      name,
		},
		GVR:           kubeproxy.NewGroupVersionResource(*r.gvr),
		DeleteOptions: &options,
		SubResources:  subresources,
	}

	_, err := r.requestUntilTimeout(ctx, deleteInfo)
	return err
}
func (r *OperateResource[R]) Get(ctx context.Context, name string, options metav1.GetOptions,
	subresources ...string) (*R, error) {
	getInfo := &kubeproxy.Get{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
			Name:      name,
		},
		GVR:          kubeproxy.NewGroupVersionResource(*r.gvr),
		GetOptions:   &options,
		SubResources: subresources,
	}
	return r.requestUntilTimeout(ctx, getInfo)
}

func (r *OperateResource[R]) BlockGet(ctx context.Context, name string, options kubeproxy.BlockGetOptions,
	subresources ...string) (*R, error) {
	getInfo := &kubeproxy.BlockGet{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
			Name:      name,
		},
		GVR:          kubeproxy.NewGroupVersionResource(*r.gvr),
		GetOptions:   &options,
		SubResources: subresources,
	}
	return r.requestUntilTimeout(ctx, getInfo)
}

func (r *OperateResource[R]) UnlockResource(_ context.Context, name string) error {
	unlockInfo := &kubeproxy.UnlockResource{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
			Name:      name,
		},
		GVR: kubeproxy.NewGroupVersionResource(*r.gvr),
	}

	future := r.ctx.RequestFuture(r.target, unlockInfo, r.timeout)
	result, err := future.Result()
	if err != nil {
		return err
	}

	resp, ok := result.(*kubeproxy.Response)
	if !ok {
		return fmt.Errorf("expected type *kubeproxy.Response, but get %T", result)
	}

	if resp.Error != nil {
		return resp.Error
	}

	return nil
}

func (r *OperateResource[R]) List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	listInfo := &kubeproxy.List{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
		},
		GVR:         kubeproxy.NewGroupVersionResource(*r.gvr),
		ListOptions: &opts,
	}

	future := r.ctx.RequestFuture(r.target, listInfo, r.timeout)
	return requestUntilTimeout[unstructured.UnstructuredList](ctx, future)
}

func (r *OperateResource[R]) ListSlice(ctx context.Context, opts metav1.ListOptions) ([]*R, error) {
	list, err := r.List(ctx, opts)
	if err != nil {
		return nil, err
	}

	if list == nil || len(list.Items) == 0 {
		return nil, nil
	}

	ret := make([]*R, 0, len(list.Items))
	for _, item := range list.Items {
		raw, err := item.MarshalJSON()
		if err != nil {
			return nil, err
		}

		var resourceItem R
		err = json.Unmarshal(raw, &resourceItem)
		if err != nil {
			return nil, err
		}

		ret = append(ret, &resourceItem)
	}
	return ret, nil
}

func (r *OperateResource[R]) Patch(ctx context.Context, name string, pt types.PatchType, data []byte,
	options metav1.PatchOptions, subresources ...string) (*R, error) {
	code, ok := kubeproxy.PatchTypeToCode[pt]
	if !ok {
		return nil, fmt.Errorf("wrong patch type %s", pt)
	}
	patchInfo := &kubeproxy.Patch{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
			Name:      name,
		},
		GVR:          kubeproxy.NewGroupVersionResource(*r.gvr),
		PatchType:    code,
		Resource:     data,
		PatchOptions: &options,
		SubResources: subresources,
	}
	return r.requestUntilTimeout(ctx, patchInfo)
}

func (r *OperateResource[R]) PatchStatus(ctx context.Context, name string, pt types.PatchType, data []byte,
	options metav1.PatchOptions) (*R, error) {
	code, ok := kubeproxy.PatchTypeToCode[pt]
	if !ok {
		return nil, fmt.Errorf("wrong patch type %s", pt)
	}
	patchInfo := &kubeproxy.PatchStatus{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
			Name:      name,
		},
		GVR:          kubeproxy.NewGroupVersionResource(*r.gvr),
		PatchType:    code,
		Resource:     data,
		PatchOptions: &options,
	}
	return r.requestUntilTimeout(ctx, patchInfo)
}

func (r *OperateResource[R]) Synchronize(ctx context.Context, original, synchronizing *R, options metav1.UpdateOptions) (*R, error) {
	rawSynchronizing, err := marshal[R](synchronizing)
	if err != nil {
		return nil, err
	}

	rawOriginal, err := marshal[R](original)
	if err != nil {
		return nil, err
	}

	updateInfo := &kubeproxy.Synchronize{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
		},
		GVR:           kubeproxy.NewGroupVersionResource(*r.gvr),
		UpdateOptions: &options,
		Synchronizing: rawSynchronizing,
		Original:      rawOriginal,
	}

	return r.requestUntilTimeout(ctx, updateInfo)
}

// Watch watches resource.
// TODO: add a watch
func (r *OperateResource[R]) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return nil, nil
}

func (r *OperateResource[R]) Namespace(namespace string) ResourceOperator[R] {
	r.namespace = namespace
	return r
}

func (r *OperateResource[R]) requestUntilTimeout(ctx context.Context, message interface{}) (*R, error) {
	future := r.ctx.RequestFuture(r.target, message, r.timeout)
	return requestUntilTimeout[R](ctx, future)
}

func requestUntilTimeout[R any](ctx context.Context, future *actor.Future) (*R, error) {
	var result any
	var err error
	resultDone := make(chan struct{})

	go func() {
		result, err = future.Result()
		resultDone <- struct{}{}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("kubernetes operator request time out")
		case <-resultDone:
			return getResult[R](result, err)
		}
	}
}

func getResult[R any](result any, err error) (*R, error) {
	resp, ok := result.(*kubeproxy.Response)
	if !ok {
		return nil, fmt.Errorf("expected type *kubeproxy.Response, but get %T", result)
	}

	if resp.Error != nil {
		return nil, fmt.Errorf("resource %s named %s/%s occurs error %v", resp.GVR.String(),
			resp.Metadata.GetNamespace(), resp.Metadata.GetName(), resp.Error)
	}

	if resp.Resource == nil {
		return nil, nil
	}
	var ret R
	err = json.Unmarshal(resp.Resource, &ret)
	if err != nil {
		return nil, err
	}

	return &ret, nil
}

func marshal[R any](obj *R) ([]byte, error) {
	if obj == nil {
		return nil, fmt.Errorf("can not create resource due to a nil object")
	}

	rawResource, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	return rawResource, nil
}
