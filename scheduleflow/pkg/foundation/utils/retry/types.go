package retry

import "time"

const (
	defaultRetryDelay = 30 * time.Second
	maximumRetryTimes = 10
)

type queueConstraint[T any] struct {
	recorder RecordKey[T]
}

func (q *queueConstraint[T]) FormStoreKey(value *retryItem[T]) string {
	return q.recorder(value.task)
}

func (q *queueConstraint[T]) Less(_, _ *retryItem[T]) bool {
	return false
}

type DefaultRetryPolicy[T any] struct {
}

func (d *DefaultRetryPolicy[T]) BackoffTime(task *T, retryTimes int) time.Duration {
	return defaultRetryDelay
}

func (d *DefaultRetryPolicy[T]) ForgetItem(task *T, retryTimes int) bool {
	if retryTimes >= maximumRetryTimes {
		return true
	}
	return false
}

type retryFunc[T any] struct {
	function func(task *T) error
}

func (retry *retryFunc[T]) RetryOnError(task *T) error {
	return retry.function(task)
}

func NewRetryFunc[T any](function func(task *T) error) RetryableExecutor[T] {
	return &retryFunc[T]{function: function}
}

type retryPolicy[T any] struct {
	backoff func(task *T, retryTimes int) time.Duration
	forget  func(task *T, retryTimes int) bool
}

func (retry *retryPolicy[T]) BackoffTime(task *T, retryTimes int) time.Duration {
	return retry.backoff(task, retryTimes)
}

func (retry *retryPolicy[T]) ForgetItem(task *T, retryTimes int) bool {
	return retry.forget(task, retryTimes)
}

func NewRetryPolicy[T any](backoff func(task *T, retryTimes int) time.Duration, forget func(task *T, retryTimes int) bool) RetryPolicy[T] {
	return &retryPolicy[T]{
		backoff: backoff,
		forget:  forget,
	}
}
