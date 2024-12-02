package operator

import (
	"fmt"

	cmap "github.com/orcaman/concurrent-map"
	ants "github.com/panjf2000/ants/v2"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/informer"
)

const (
	defaultCreationPool = 20
	defaultUpdatingPool = 100
)

type resourceOperator struct {
	dynamic.Interface
	gvr   schema.GroupVersionResource
	store informer.UnstructuredOperableStore

	updateLocker cmap.ConcurrentMap[*resourceCond]
	creationPool *ants.Pool
	updatingPool *ants.Pool
}

func newOperatorAPI(gvr *kubeproxy.GroupVersionResource, client dynamic.Interface, creationPool, updatingPool int) *resourceOperator {
	if creationPool == 0 {
		creationPool = defaultCreationPool
	}

	if updatingPool == 0 {
		updatingPool = defaultUpdatingPool
	}

	cPool, err := ants.NewPool(creationPool)
	if err != nil {
		logrus.Fatal(err)
	}

	uPool, err := ants.NewPool(creationPool)
	if err != nil {
		logrus.Fatal(err)
	}

	return &resourceOperator{
		gvr:          kubeproxy.ConvertGVR(gvr.DeepCopy()),
		updateLocker: cmap.New[*resourceCond](),
		Interface:    client,
		creationPool: cPool,
		updatingPool: uPool,
	}
}

// Name records resource operator manages resource name.
func (ope *resourceOperator) Name() string {
	return kubeproxy.NewGroupVersionResource(ope.gvr).String()
}

// CanProcess trigger processor processing
func (ope *resourceOperator) CanProcess(msg interface{}) bool {
	switch m := msg.(type) {
	case kubeproxy.KubernetesAPIBase:
		if m.GetGVR().String() == ope.Name() {
			return true
		}
		return false
	default:
		return false
	}
}

// Process deal with message
func (ope *resourceOperator) Process(ctx actor.Context, env *actor.MessageEnvelope) (interface{}, error) {
	switch env.Message.(type) {
	case kubeproxy.KubernetesAPIBase:
		envelop := &actor.MessageEnvelope{
			Message: env.Message,
			Sender:  env.Sender,
		}
		return ope.rateLimitingOperatingResource(ctx, envelop)
	default:
		return nil, fmt.Errorf("operate resource type error: expect kubeproxy.KubernetesAPIBase, but get %T", env.Message)
	}
}

func (ope *resourceOperator) rateLimitingOperatingResource(ctx actor.Context, env *actor.MessageEnvelope) (interface{}, error) {
	switch env.Message.(type) {
	case *kubeproxy.Create:
		return ope.limitingOperate(ctx, env, ope.creationPool)
	case *kubeproxy.PatchStatus, *kubeproxy.Synchronize, *kubeproxy.Update, *kubeproxy.UpdateStatus:
		return ope.limitingOperate(ctx, env, ope.updatingPool)
	}

	go ope.operateResource(ctx, env)
	return nil, nil
}

func (ope *resourceOperator) limitingOperate(ctx actor.Context, env *actor.MessageEnvelope, pool *ants.Pool) (interface{}, error) {
	if pool.IsClosed() {
		return nil, fmt.Errorf("resource operator %v can not create resource due to creation pool close ", ope.gvr)
	}

	info, ok := env.Message.(kubeproxy.KubernetesAPIBase)
	if !ok {
		return nil, fmt.Errorf("message expected kubeproxy.KubernetesAPIBase, but get %T", env.Message)
	}

	if pool.Free() == 0 {
		return createKubernetesAPIErrorResponse(info, fmt.Errorf("too many request, please try later")), nil
	}

	err := pool.Submit(func() { ope.operateResource(ctx, env) })
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (ope *resourceOperator) operateResource(ctx actor.Context, env *actor.MessageEnvelope) {
	var rep *kubeproxy.Response
	var err error
	switch m := env.Message.(type) {
	case *kubeproxy.Get:
		rep, err = ope.get(m)
	case *kubeproxy.BlockGet:
		rep, err = ope.blockGet(m)
	case *kubeproxy.Create:
		rep, err = ope.create(m)
	case *kubeproxy.List:
		rep, err = ope.list(m)
	case *kubeproxy.Delete:
		rep, err = ope.delete(m)
	case *kubeproxy.Patch:
		rep, err = ope.patch(m)
	case *kubeproxy.PatchStatus:
		rep, err = ope.patchStatus(m)
	case *kubeproxy.Synchronize:
		rep, err = ope.synchronize(m)
	case *kubeproxy.Update:
		rep, err = ope.update(m)
	case *kubeproxy.UpdateStatus:
		rep, err = ope.updateStatus(m)
	case *kubeproxy.UnlockResource:
		rep, err = ope.unlockResource(m)
	}
	if err != nil {
		logrus.Errorf("operate resource with error: %v", err)
		return
	}
	ctx.Send(env.Sender, rep)
}

func (ope *resourceOperator) informResourceAdd(resource unstructured.Unstructured) {
	cond, ok := ope.updateLocker.Get(formResourceKey(&resource))
	if !ok {
		return
	}
	cond.Broadcast()
}

func (ope *resourceOperator) informResourceUpdate(_, newOld unstructured.Unstructured) {
	cond, ok := ope.updateLocker.Get(formResourceKey(&newOld))
	if !ok {
		return
	}
	cond.Broadcast()
}

func (ope *resourceOperator) informResourceDelete(resource unstructured.Unstructured) {
	cond, ok := ope.updateLocker.Get(formResourceKey(&resource))
	if !ok {
		return
	}
	ope.updateLocker.Remove(formResourceKey(&resource))
	cond.Broadcast()
}

func (ope *resourceOperator) unlockResource(info *kubeproxy.UnlockResource) (*kubeproxy.Response, error) {
	resource := &unstructured.Unstructured{Object: make(map[string]interface{})}
	resource.SetNamespace(info.GetMetadata().Namespace)
	resource.SetName(info.GetMetadata().Name)
	cond, ok := ope.updateLocker.Get(formResourceKey(resource))
	if !ok {
		noFind := fmt.Errorf("resource %s/%s is not locked", info.GetMetadata().Namespace, info.GetMetadata().Name)
		return createKubernetesAPIErrorResponse(info, noFind), nil
	}

	cond.Broadcast()
	return createKubernetesAPIResponse(info, nil), nil
}

func formResourceKey(resource *unstructured.Unstructured) string {
	if resource == nil {
		return ""
	}
	return resource.GetNamespace() + "/" + resource.GetName()
}
