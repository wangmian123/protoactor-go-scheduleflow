package coresync

import (
	"time"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils/retry"
	cmap "github.com/orcaman/concurrent-map"
)

type (
	CreationStreamChannel[T any] chan *T
	UpdatingStreamChannel[T any] chan *ResourceUpdater[T]

	RecordKey[S any] func(object *S) string
	Option[S, T any] func(option *option[S, T])
)

const maximumUpdateInterval = 5 * time.Minute

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

type SynchronizerType int

const (
	BilateralSync SynchronizerType = iota
	UpstreamSync
	DownstreamSync
)

func WithDownstreamMinimumUpdateInterval[S, T any](interval time.Duration) Option[S, T] {
	return func(option *option[S, T]) {
		option.minimumDownstreamUpdateInterval = interval
	}
}

func WithDownstreamMaximumUpdateInterval[S, T any](interval time.Duration) Option[S, T] {
	return func(option *option[S, T]) {
		option.maximumDownstreamUpdateInterval = interval
	}
}

func WithUpstreamMinimumUpdateInterval[S, T any](interval time.Duration) Option[S, T] {
	return func(option *option[S, T]) {
		option.minimumUpstreamUpdateInterval = interval
	}
}

func WithUpstreamMaximumUpdateInterval[S, T any](interval time.Duration) Option[S, T] {
	return func(option *option[S, T]) {
		option.maximumUpstreamUpdateInterval = interval
	}
}

func WithDownstreamCreationRetryPolicy[S, T any](backoffTime func(task *T, retryTimes int) time.Duration,
	forgetItem func(task *T, retryTimes int) bool) Option[S, T] {
	convertBackoff := func(task *creatingStream[S, T], retryTimes int) time.Duration {
		return backoffTime(task.incoming, retryTimes)
	}

	convertForgetItem := func(task *creatingStream[S, T], retryTimes int) bool {
		return forgetItem(task.incoming, retryTimes)
	}

	return func(option *option[S, T]) {
		option.downstreamCreationRetryPolicy = retry.NewRetryPolicy[creatingStream[S, T]](convertBackoff, convertForgetItem)
	}
}

func WithDownstreamUpdatingRetryPolicy[S, T any](backoffTime func(task *T, retryTimes int) time.Duration,
	forgetItem func(task *T, retryTimes int) bool) Option[S, T] {
	convertBackoff := func(task *updatingStream[S, T], retryTimes int) time.Duration {
		return backoffTime(task.incoming.Newest, retryTimes)
	}

	convertForgetItem := func(task *updatingStream[S, T], retryTimes int) bool {
		return forgetItem(task.incoming.Newest, retryTimes)
	}

	return func(option *option[S, T]) {
		option.downstreamUpdatingRetryPolicy = retry.NewRetryPolicy[updatingStream[S, T]](convertBackoff, convertForgetItem)
	}
}

func WithUpstreamCreationRetryPolicy[S, T any](backoffTime func(task *S, retryTimes int) time.Duration,
	forgetItem func(task *S, retryTimes int) bool) Option[S, T] {
	convertBackoff := func(task *creatingStream[S, T], retryTimes int) time.Duration {
		return backoffTime(task.source, retryTimes)
	}

	convertForgetItem := func(task *creatingStream[S, T], retryTimes int) bool {
		return forgetItem(task.source, retryTimes)
	}

	return func(option *option[S, T]) {
		option.upstreamCreationRetryPolicy = retry.NewRetryPolicy[creatingStream[S, T]](convertBackoff, convertForgetItem)
	}
}

func WithUpstreamUpdatingRetryPolicy[S, T any](backoffTime func(task *S, retryTimes int) time.Duration,
	forgetItem func(task *S, retryTimes int) bool) Option[S, T] {
	convertBackoff := func(task *updatingStream[S, T], retryTimes int) time.Duration {
		return backoffTime(task.source.Newest, retryTimes)
	}

	convertForgetItem := func(task *updatingStream[S, T], retryTimes int) bool {
		return forgetItem(task.source.Newest, retryTimes)
	}

	return func(option *option[S, T]) {
		option.upstreamUpdatingRetryPolicy = retry.NewRetryPolicy[updatingStream[S, T]](convertBackoff, convertForgetItem)
	}
}

type bindResource[S, T any] struct {
	source       *S
	targets      cmap.ConcurrentMap[*T]
	latestUpdate *time.Time
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

type constraintUpdateKey[T any] struct {
	recorder RecordKey[T]
}

func newConstraintUpdateKey[T any](recorder RecordKey[T]) *constraintUpdateKey[T] {
	return &constraintUpdateKey[T]{recorder: recorder}
}

func (c *constraintUpdateKey[T]) FormStoreKey(updater *ResourceUpdater[T]) string {
	return c.recorder(updater.Newest)
}

func (c *constraintUpdateKey[T]) Less(*ResourceUpdater[T], *ResourceUpdater[T]) bool {
	return false
}

type option[S, T any] struct {
	downstreamCreationRetryPolicy retry.RetryPolicy[creatingStream[S, T]]
	downstreamUpdatingRetryPolicy retry.RetryPolicy[updatingStream[S, T]]
	upstreamCreationRetryPolicy   retry.RetryPolicy[creatingStream[S, T]]
	upstreamUpdatingRetryPolicy   retry.RetryPolicy[updatingStream[S, T]]

	maximumDownstreamUpdateInterval time.Duration
	maximumUpstreamUpdateInterval   time.Duration
	minimumDownstreamUpdateInterval time.Duration
	minimumUpstreamUpdateInterval   time.Duration
}

func newOption[S, T any](opts ...Option[S, T]) *option[S, T] {
	defaultOption := &option[S, T]{
		downstreamCreationRetryPolicy: &retry.DefaultRetryPolicy[creatingStream[S, T]]{},
		downstreamUpdatingRetryPolicy: &retry.DefaultRetryPolicy[updatingStream[S, T]]{},
		upstreamCreationRetryPolicy:   &retry.DefaultRetryPolicy[creatingStream[S, T]]{},
		upstreamUpdatingRetryPolicy:   &retry.DefaultRetryPolicy[updatingStream[S, T]]{},

		maximumDownstreamUpdateInterval: maximumUpdateInterval,
		maximumUpstreamUpdateInterval:   maximumUpdateInterval,
	}

	for _, opt := range opts {
		opt(defaultOption)
	}

	return defaultOption
}

type recorder[S, T any] struct {
	upstreamRecorder   RecordKey[S]
	downstreamRecorder RecordKey[T]
}
