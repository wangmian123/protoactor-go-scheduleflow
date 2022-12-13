package retry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/LiuYuuChen/algorithms/queue"
	"github.com/sirupsen/logrus"
)

type RecordKey[T any] func(*T) string

type retryItem[T any] struct {
	retryTimes int
	task       *T
}

type RetryableExecutor[T any] interface {
	RetryOnError(task *T) error
}

type RetryPolicy[T any] interface {
	BackoffTime(task *T, retryTimes int) time.Duration
	ForgetItem(task *T, retryTimes int) bool
}

type RetryableQueue[T any] interface {
	Run(ctx context.Context)
	EnqueueTask(task *T) error
	EnqueueTaskAfter(task *T, interval time.Duration) error
	UpdateTask(task *T) error
	DequeueTask(task *T) error
}

type retryableQueue[T any] struct {
	closed bool
	once   *sync.Once

	keyFunc     RecordKey[T]
	retryQueue  queue.DelayingQueue[*retryItem[T]]
	retryFunc   RetryableExecutor[T]
	retryPolicy RetryPolicy[T]
}

func New[T any](keyFunc RecordKey[T], retryFunc RetryableExecutor[T], policy RetryPolicy[T]) RetryableQueue[T] {
	return &retryableQueue[T]{
		retryQueue: queue.NewDelayingQueue[*retryItem[T]](&queueConstraint[T]{
			recorder: keyFunc,
		}),
		retryFunc:   retryFunc,
		retryPolicy: policy,
		once:        &sync.Once{},
	}
}

func NewWithDefaultRetryPolicy[T any](keyFunc RecordKey[T], retryFunc RetryableExecutor[T]) RetryableQueue[T] {
	return &retryableQueue[T]{
		retryQueue: queue.NewDelayingQueue[*retryItem[T]](&queueConstraint[T]{
			recorder: keyFunc,
		}),
		retryFunc:   retryFunc,
		retryPolicy: &DefaultRetryPolicy[T]{},
	}
}

func (q *retryableQueue[T]) EnqueueTask(task *T) error {
	if q.closed || q.retryQueue.IsShutdown() {
		return fmt.Errorf("task queue has been closed")
	}
	q.retryQueue.Add(&retryItem[T]{task: task})
	return nil
}

func (q *retryableQueue[T]) EnqueueTaskAfter(task *T, interval time.Duration) error {
	if q.closed || q.retryQueue.IsShutdown() {
		return fmt.Errorf("task queue has been closed")
	}
	q.retryQueue.AddAfter(&retryItem[T]{task: task}, interval)
	return nil
}

func (q *retryableQueue[T]) UpdateTask(task *T) error {
	if q.closed || q.retryQueue.IsShutdown() {
		return fmt.Errorf("task queue has been closed")
	}
	return q.retryQueue.Update(&retryItem[T]{task: task})
}

func (q *retryableQueue[T]) DequeueTask(task *T) error {
	if q.closed || q.retryQueue.IsShutdown() {
		return fmt.Errorf("task queue has been closed")
	}
	_ = q.retryQueue.Delete(&retryItem[T]{task: task})
	return nil
}

func (q *retryableQueue[T]) run(ctx context.Context) {
	for {
		select {
		case _, ok := <-ctx.Done():
			if !ok {
				logrus.Errorf("retryable context unexpected closed.")
				return
			}
			q.closed = true
			q.retryQueue.Shutdown()
			return
		default:
		}

		item, err := q.retryQueue.Pop()
		if err != nil {
			if err != nil {
				logrus.Errorf("retryable task queue pop error due to %v", err)
			}
		}

		err = q.retryFunc.RetryOnError(item.task)
		if err != nil {
			item.retryTimes++
			if q.retryPolicy.ForgetItem(item.task, item.retryTimes) {
				continue
			}
			backoffTime := q.retryPolicy.BackoffTime(item.task, item.retryTimes)
			q.retryQueue.AddAfter(item, backoffTime)
		}
	}
}

func (q *retryableQueue[T]) Run(ctx context.Context) {
	q.once.Do(func() {
		q.run(ctx)
	})
}
