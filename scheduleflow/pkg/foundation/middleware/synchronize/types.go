package synchronize

import (
	"fmt"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/informer"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/kubernetes"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/synchronize/coresync"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const ClusterLabel = "cluster.scheduleflow.io"

type (
	DynamicLabelObject       = LabeledObject[unstructured.Unstructured]
	DynamicResourceUpdater   = coresync.ResourceUpdater[LabeledObject[unstructured.Unstructured]]
	DynamicDownstreamTrigger = coresync.DownstreamTrigger[DynamicLabelObject, DynamicLabelObject]
	DynamicUpstreamTrigger   = coresync.UpstreamTrigger[DynamicLabelObject, DynamicLabelObject]
	DynamicInformerConvertor = LabeledInformerConvertor[unstructured.Unstructured]
	DynamicSynchronizer      = GenericSynchronizer[unstructured.Unstructured, unstructured.Unstructured]
	DynamicMutatedResult     = MutateResourceResult[unstructured.Unstructured]
	DynamicAssignment        = GenericAssignment[unstructured.Unstructured, unstructured.Unstructured]
	DynamicResourceMutator   = ResourceMutator[unstructured.Unstructured, unstructured.Unstructured]
)

type MutatingAction string

const (
	Ignore       MutatingAction = ""
	Create                      = "Create"
	Patch                       = "Patch"
	PatchStatus                 = "PatchStatus"
	Update                      = "Update"
	UpdateStatus                = "UpdateStatus"
	Synchronize                 = "Synchronize"
	Delete                      = "Delete"
)

type MutateResourceResult[R any] struct {
	Action           MutatingAction
	MutatedResource  *R
	OriginalResource *R
}

type Operation struct {
	MappingNamespace *string
	MappingNames     map[string]string
}

type SynchronizerObject struct {
	GVR schema.GroupVersionResource

	ClusterInformer *actor.PID
	ClusterK8sAPI   *actor.PID
}

func NewSynchronizeObject(ipPort string, gvr schema.GroupVersionResource) SynchronizerObject {
	return SynchronizerObject{
		ClusterInformer: informer.GetPID(ipPort),
		ClusterK8sAPI:   kubernetes.GetAPI(ipPort),
		GVR:             gvr,
	}
}

type DynamicAddedMutating func(added, todo *unstructured.Unstructured) (*DynamicMutatedResult, error)
type DynamicUpdatedMutating func(updatedOld, updatedNew, todo *unstructured.Unstructured) (*DynamicMutatedResult, error)
type DynamicDeletedMutating func(deleted, todo *unstructured.Unstructured) (*DynamicMutatedResult, error)

type DynamicResourceMutatorImp struct {
	WhenAdd    DynamicAddedMutating
	WhenUpdate DynamicUpdatedMutating
	WhenDelete DynamicDeletedMutating
}

func (dy *DynamicResourceMutatorImp) SynchronizeAddedResource(added *unstructured.Unstructured, todo *unstructured.Unstructured,
) (*MutateResourceResult[unstructured.Unstructured], error) {
	return dy.WhenAdd(added, todo)
}

func (dy *DynamicResourceMutatorImp) SynchronizeUpdatedResource(updatedOld, updatedNew *unstructured.Unstructured,
	todo *unstructured.Unstructured) (*MutateResourceResult[unstructured.Unstructured], error) {
	return dy.WhenUpdate(updatedOld, updatedNew, todo)
}

func (dy *DynamicResourceMutatorImp) SynchronizeDeletedResource(deleted *unstructured.Unstructured, todo *unstructured.Unstructured,
) (*MutateResourceResult[unstructured.Unstructured], error) {
	return dy.WhenDelete(deleted, todo)
}

type AddedMutating[S, T any] func(added *S, todo *T) (*MutateResourceResult[T], error)
type UpdatedMutating[S, T any] func(updatedOld, updatedNew *S, todo *T) (*MutateResourceResult[T], error)
type DeletedMutating[S, T any] func(deleted *S, todo *T) (*MutateResourceResult[T], error)

type ResourceMutatorImp[S, T any] struct {
	WhenAdd    AddedMutating[S, T]
	WhenUpdate UpdatedMutating[S, T]
	WhenDelete DeletedMutating[S, T]
}

func (r *ResourceMutatorImp[S, T]) SynchronizeAddedResource(added *S, todo *T) (*MutateResourceResult[T], error) {
	return r.WhenAdd(added, todo)
}

func (r *ResourceMutatorImp[S, T]) SynchronizeUpdatedResource(updatedOld, updatedNew *S, todo *T) (*MutateResourceResult[T], error) {
	return r.WhenUpdate(updatedOld, updatedNew, todo)
}

func (r *ResourceMutatorImp[S, T]) SynchronizeDeletedResource(deleted *S, todo *T) (*MutateResourceResult[T], error) {
	return r.WhenDelete(deleted, todo)
}

type ChangeType string

const (
	ResourceAdd    ChangeType = "ResourceAdd"
	ResourceUpdate ChangeType = "ResourceUpdate"
	ResourceDelete ChangeType = "ResourceDelete"
)

type ResourceChanging struct {
	Type        ChangeType
	Resource    *unstructured.Unstructured
	OldResource *unstructured.Unstructured
}

type ResourceChangingStoreConstraint struct {
}

func (c *ResourceChangingStoreConstraint) FormStoreKey(res *ResourceChanging) string {
	if res.Resource == nil {
		return ""
	}
	return res.Resource.GetNamespace() + "/" + res.Resource.GetName()
}

func (c *ResourceChangingStoreConstraint) Less(*ResourceChanging, *ResourceChanging) bool {
	return false
}

func ConvertMutateResult[R, T any](res *MutateResourceResult[R]) (*MutateResourceResult[T], error) {
	if res == nil {
		return nil, nil
	}

	ret := &MutateResourceResult[T]{
		Action: res.Action,
	}

	if res.MutatedResource != nil {
		mutated, err := utils.ResourceConvert[R, T](res.MutatedResource)
		if err != nil {
			return nil, err
		}
		ret.MutatedResource = mutated
	}

	if res.OriginalResource != nil {
		origin, err := utils.ResourceConvert[R, T](res.OriginalResource)
		if err != nil {
			return nil, err
		}
		ret.OriginalResource = origin
	}

	return ret, nil
}

type LabeledObject[T any] struct {
	Label    map[string]string
	Resource *T
}

type LabeledInformerConvertor[T any] interface {
	CreateLabeledInformer() coresync.Informer[LabeledObject[T]]
	CreateDynamicEventHandler() informer.DynamicEventHandler
}

type ObjectLabeler[T any] interface {
	LabelWhenResourceAdd(resource *T) map[string]string
	LabelWhenResourceUpdate(oldResource, newResource *T) (map[string]string, map[string]string)
	LabelWhenResourceDelete(resource *T) map[string]string
}

type ResourceGetter[T any] interface {
	Get(target *T) (*T, bool)
	List() []*T
}

type Cluster struct {
	Address string
	Name    string
}

func (c *Cluster) LabelCluster() string {
	return c.Address
}

func formClusterResourceKey(clusterLabel, namespace, name string) string {
	format := "%s/%s/%s/%s"
	return fmt.Sprintf(format, ClusterLabel, clusterLabel, namespace, name)
}
