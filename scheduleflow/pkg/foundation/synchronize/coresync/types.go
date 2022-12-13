package coresync

import (
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils/retry"
	cmap "github.com/orcaman/concurrent-map"
)

type (
	CreationStreamChannel[T any] chan *T
	UpdatingStreamChannel[T any] chan *ResourceUpdater[T]
	RecordKey[S any]             func(object *S) string
	SynchronizerOption[S, T any] func(option *synchronizerOption[S, T])
)

type creationType int

const (
	upstreamInflow creationType = iota
	upstreamOutflow
	downstreamInflow
	downstreamOutflow
)

type updatingType int

const (
	upstreamUpdate updatingType = iota
	downstreamUpdate
)

type synchronizerOption[S, T any] struct {
	creatingRetry retry.RetryPolicy[creatingStream[S, T]]
	updatingRetry retry.RetryPolicy[updatingStream[S, T]]
}

func newOption[S, T any]() *synchronizerOption[S, T] {
	return &synchronizerOption[S, T]{
		creatingRetry: &retry.DefaultRetryPolicy[creatingStream[S, T]]{},
		updatingRetry: &retry.DefaultRetryPolicy[updatingStream[S, T]]{},
	}
}

type bindResource[S, T any] struct {
	source  *S
	targets cmap.ConcurrentMap[*T]
}

type creatingStream[S, T any] struct {
	source   *S
	incoming *T
	targets  map[string]*T
	taskType creationType
}

type updatingStream[S, T any] struct {
	source   *ResourceUpdater[S]
	incoming *ResourceUpdater[T]
	targets  map[string]*T
	taskType updatingType
}

type ResourceUpdater[T any] struct {
	ProbableOld *T
	Newest      *T
	JsonDiff    []byte
}
