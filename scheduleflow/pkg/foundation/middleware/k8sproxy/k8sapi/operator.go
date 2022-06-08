package k8sapi

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/k8sproxy"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

type OperateResource[R any] struct {
	ctx       actor.Context
	target    *actor.PID
	gvr       *schema.GroupVersionResource
	namespace string
	timeout   time.Duration
}

func NewOperator[R any](ctx actor.Context, target *actor.PID, gvr *schema.GroupVersionResource, opts ...Option) *OperateResource[R] {
	cfg := config{}
	cfg.timeout = timeout
	for _, opt := range opts {
		opt(&cfg)
	}

	return &OperateResource[R]{
		ctx:       ctx,
		target:    target,
		gvr:       gvr,
		namespace: metav1.NamespaceAll,
		timeout:   cfg.timeout,
	}
}

func (r *OperateResource[R]) Create(ctx context.Context, obj *R, options metav1.CreateOptions, subresources ...string) (*R, error) {
	rawResource, err := marshal[R](obj)
	if err != nil {
		return nil, err
	}

	createInfo := &k8sproxy.Create{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
		},
		GVR:           k8sproxy.NewGroupVersionResource(*r.gvr),
		CreateOptions: &options,
		Resource:      rawResource,
		SubResources:  subresources,
	}

	return r.requestUntilTimeout(ctx, createInfo)
}

func (r *OperateResource[R]) Update(ctx context.Context, obj *R, options metav1.UpdateOptions, subresources ...string) (*R, error) {
	rawResource, err := marshal[R](obj)
	if err != nil {
		return nil, err
	}

	updateInfo := &k8sproxy.Update{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
		},
		GVR:           k8sproxy.NewGroupVersionResource(*r.gvr),
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

	updateInfo := &k8sproxy.UpdateStatus{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
		},
		GVR:           k8sproxy.NewGroupVersionResource(*r.gvr),
		UpdateOptions: &options,
		Resource:      rawResource,
	}

	return r.requestUntilTimeout(ctx, updateInfo)
}

func (r *OperateResource[R]) Delete(ctx context.Context, name string, options metav1.DeleteOptions, subresources ...string) error {
	deleteInfo := &k8sproxy.Delete{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
			Name:      name,
		},
		GVR:           k8sproxy.NewGroupVersionResource(*r.gvr),
		DeleteOptions: &options,
		SubResources:  subresources,
	}

	_, err := r.requestUntilTimeout(ctx, deleteInfo)
	return err
}

func (r *OperateResource[R]) Get(ctx context.Context, name string, options metav1.GetOptions, subresources ...string) (*R, error) {
	getInfo := &k8sproxy.Get{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
			Name:      name,
		},
		GVR:          k8sproxy.NewGroupVersionResource(*r.gvr),
		GetOptions:   &options,
		SubResources: subresources,
	}
	return r.requestUntilTimeout(ctx, getInfo)
}

func (r *OperateResource[R]) List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	listInfo := &k8sproxy.List{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
		},
		GVR:         k8sproxy.NewGroupVersionResource(*r.gvr),
		ListOptions: &opts,
	}

	future := r.ctx.RequestFuture(r.target, listInfo, r.timeout)
	return requestUntilTimeout[unstructured.UnstructuredList](ctx, future)
}

func (r *OperateResource[R]) ListSlice(ctx context.Context, opts metav1.ListOptions) ([]R, error) {
	list, err := r.List(ctx, opts)
	if err != nil {
		return nil, err
	}

	if list == nil || len(list.Items) == 0 {
		return nil, nil
	}

	ret := make([]R, 0, len(list.Items))
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

		ret = append(ret, resourceItem)
	}
	return ret, nil
}

func (r *OperateResource[R]) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, options metav1.PatchOptions, subresources ...string) (*R, error) {
	code, ok := k8sproxy.PatchTypeToCode[pt]
	if !ok {
		return nil, fmt.Errorf("wrong patch type %s", pt)
	}
	patchInfo := &k8sproxy.Patch{
		Metadata: &metav1.ObjectMeta{
			Namespace: r.namespace,
			Name:      name,
		},
		GVR:          k8sproxy.NewGroupVersionResource(*r.gvr),
		PatchType:    code,
		Resource:     data,
		PatchOptions: &options,
		SubResources: subresources,
	}
	return r.requestUntilTimeout(ctx, patchInfo)
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
	errCh := make(chan error, 1)
	resCh := make(chan R, 1)

	go func() {
		result, err := future.Result()
		if err != nil {
			errCh <- err
			return
		}

		resp, ok := result.(*k8sproxy.Response)
		if !ok {
			errCh <- fmt.Errorf("expected type *k8sproxy.Response, but get %T", result)
			return
		}

		if resp.Error != nil {
			errCh <- resp.Error
			return
		}

		var ret R

		if resp.Resource == nil {
			resCh <- ret
			return
		}

		err = json.Unmarshal(resp.Resource, &ret)
		if err != nil {
			resCh <- ret
		}
	}()

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("request timeout")
	case msg := <-errCh:
		return nil, msg
	case res := <-resCh:
		return &res, nil
	}
}

func marshal[R any](obj *R) ([]byte, error) {
	if obj == nil {
		return nil, fmt.Errorf("can not create resource due to a nil object")
	}

	rawResource, err := json.Marshal(*obj)
	if err != nil {
		return nil, err
	}

	return rawResource, nil
}
