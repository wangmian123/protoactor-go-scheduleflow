package synchronize

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/informer"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/kubernetes"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	AllNamespace = ""
)

type (
	DynamicSynchronizer    = GenericSynchronizer[unstructured.Unstructured, unstructured.Unstructured]
	DynamicMutatedResult   = MutateResourceResult[unstructured.Unstructured]
	DynamicAssignment      = GenericAssignment[unstructured.Unstructured, unstructured.Unstructured]
	DynamicResourceMutator = ResourceMutator[unstructured.Unstructured, unstructured.Unstructured]
)

// SynchronizerFactory builds synchronizerPair with the specific clusters and resources.
type SynchronizerFactory interface {
	CreateSynchronizer(source, target SynchronizerObject, opts ...Option) (DynamicSynchronizer, error)
}

type DynamicSynchronizerBuilder interface {
	GetSourceInformerHandler() informer.DynamicEventHandler
	GetTargetInformerHandler() informer.DynamicEventHandler
	SetSourceResourceStore(store informer.UnstructuredStore) DynamicSynchronizerBuilder
	SetTargetResourceStore(store informer.UnstructuredStore) DynamicSynchronizerBuilder

	CreateSynchronizer() DynamicSynchronizer
}

// GenericSynchronizer manages synchronizing GenericAssignment, and assignment coexistence with
// associated source resource, which means if source resource deleted, GenericAssignment will be
// deleted.
type GenericSynchronizer[S, T any] interface {
	GetSynchronizeAssignment(namespace, name string) (GenericAssignment[S, T], bool)
	RemoveSynchronizeAssignment(namespace, name string)
	AddSynchronizeAssignments(op *Operation, tasks ...GenericAssignment[S, T]) error
}

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

type ResourceMutator[S, T any] interface {
	// SynchronizeAddedResource calls when resource is adding, then synchronizing pair resource
	// in terms of mutated resource in *MutateResourceResult[T].MutatedResource.
	SynchronizeAddedResource(added *S, todo *T) (*MutateResourceResult[T], error)
	// SynchronizeUpdatedResource calls when resource is updating, then synchronizing pair resource
	// in terms of mutated resource in *MutateResourceResult[T].MutatedResource.
	SynchronizeUpdatedResource(updatedOld, updatedNew *S, todo *T) (*MutateResourceResult[T], error)
	// SynchronizeDeletedResource calls when resource is deleting, then synchronizing pair resource
	// in terms of mutated resource in *MutateResourceResult[T].MutatedResource.
	SynchronizeDeletedResource(deleted *S, todo *T) (*MutateResourceResult[T], error)
}

type GenericAssignment[S, T any] interface {
	// GetName returns assignment resource name .
	GetName() string
	// GetNamespace returns assignment resource namespace.
	GetNamespace() string
	SynchronizeTarget() ResourceMutator[S, T]
	SynchronizeSource() ResourceMutator[T, S]
}

type Operation struct {
	MappingNamespace string
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
