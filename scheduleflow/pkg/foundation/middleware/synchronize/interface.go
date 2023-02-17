package synchronize

import (
	"context"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/informer"
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
	Run(ctx context.Context)
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
	GetName() string
	GetNamespace() string
	SynchronizeTarget() ResourceMutator[S, T]
	SynchronizeSource() ResourceMutator[T, S]
}
