package coresync

import (
	"context"
	"fmt"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils/retry"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
)

type upstreamer[S, T any] struct {
	Getter[S, T]
	Informer[S]
	UpstreamOperator[S, T]

	name          string
	bind          cmap.ConcurrentMap[*bindResource[S, T]]
	upRecorder    RecordKey[S]
	downRecorder  RecordKey[T]
	creatingQueue retry.RetryableQueue[creatingStream[S, T]]
	updatingQueue retry.RetryableQueue[updatingStream[S, T]]
}

func newUpstreamer[S, T any](
	informer Informer[S],
	operator UpstreamOperator[S, T],
	getter Getter[S, T],
	sRecorder RecordKey[S],
	tRecorder RecordKey[T],
	bind cmap.ConcurrentMap[*bindResource[S, T]],
	creatingRetry retry.RetryPolicy[creatingStream[S, T]],
	updatingRetry retry.RetryPolicy[updatingStream[S, T]]) *upstreamer[S, T] {
	name := fmt.Sprintf("[Synchonizer-%T-%T][Upstreamer]", *new(S), *new(T))
	sync := &upstreamer[S, T]{
		Informer:         informer,
		Getter:           getter,
		name:             name,
		UpstreamOperator: operator,
		bind:             bind,
		upRecorder:       sRecorder,
		downRecorder:     tRecorder,
	}

	creatingKeyFunc := func(task *creatingStream[S, T]) string {
		return fmt.Sprintf("%s-create-%d", sRecorder(task.source), task.taskType)
	}

	updatingKeyFunc := func(task *updatingStream[S, T]) string {
		return fmt.Sprintf("%s-update", sRecorder(task.source.Newest))
	}

	if creatingRetry == nil {
		creatingRetry = &retry.DefaultRetryPolicy[creatingStream[S, T]]{}
	}

	if updatingRetry == nil {
		updatingRetry = &retry.DefaultRetryPolicy[updatingStream[S, T]]{}
	}

	sync.creatingQueue = retry.New[creatingStream[S, T]](creatingKeyFunc, retry.NewRetryFunc[creatingStream[S, T]](sync.tryCreatingResource), creatingRetry)
	sync.updatingQueue = retry.New[updatingStream[S, T]](updatingKeyFunc, retry.NewRetryFunc[updatingStream[S, T]](sync.tryUpdatingResource), updatingRetry)
	return sync
}

func (up *upstreamer[S, T]) logPrefix() string {
	return up.name
}

func (up *upstreamer[S, T]) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case src, ok := <-up.InflowChannel():
			if !ok {
				logrus.Errorf("synchronizer inflow channel has been closed unexpected")
				continue
			}
			err := up.syncDownstreamWhenUpstreamCreating(src, upstreamInflow)
			if err != nil {
				logrus.Errorf("%s sync with error", up.logPrefix())
			}

		case src, ok := <-up.UpdateChannel():
			if !ok {
				logrus.Errorf("synchronizer update channel has been closed unexpected")
				continue
			}
			err := up.syncDownstreamWhenUpstreamUpdating(src)
			if err != nil {
				logrus.Errorf("%s sync with error", up.logPrefix())
			}

		case src, ok := <-up.OutflowChannel():
			if !ok {
				logrus.Errorf("synchronizer outflow channel has been closed unexpected")
				continue
			}

			err := up.syncDownstreamWhenUpstreamCreating(src, upstreamOutflow)
			if err != nil {
				logrus.Errorf("%s sync with error", up.logPrefix())
			}
		}
	}
}

func (up *upstreamer[S, T]) syncDownstreamWhenUpstreamCreating(source *S, taskType creationType) error {
	if source == nil {
		return fmt.Errorf("%s sync a empty Source", up.logPrefix())
	}

	task, ok := up.bind.Get(up.upRecorder(source))
	if !ok {
		return nil
	}

	switch taskType {
	case upstreamInflow, upstreamOutflow:
		break
	default:
		return fmt.Errorf("creating type error, expected upstreamInflow and upstreamOutflow")
	}

	newTask := &creatingStream[S, T]{source: source, targets: task.targets.Items(), taskType: taskType}
	err := up.creatingQueue.EnqueueTask(newTask)
	if err != nil {
		return fmt.Errorf("can not add task due to %v", err)
	}

	return nil
}

func (up *upstreamer[S, T]) syncDownstreamWhenUpstreamUpdating(source *ResourceUpdater[S]) error {
	if source == nil || source.Newest == nil {
		return fmt.Errorf("%s sync a empty Source", up.logPrefix())
	}

	task, ok := up.bind.Get(up.upRecorder(source.Newest))
	if !ok {
		return nil
	}

	newTask := &updatingStream[S, T]{source: source, targets: task.targets.Items(), taskType: upstreamUpdate}
	err := up.updatingQueue.EnqueueTask(newTask)
	if err != nil {
		return fmt.Errorf("can not add task due to %v", err)
	}

	return nil
}

func (up *upstreamer[S, T]) tryUpdatingResource(task *updatingStream[S, T]) error {
	if task == nil {
		logrus.Errorf("%s retry a nil task", up.logPrefix())
		return nil
	}

	missingTask := make(map[string]*T)
	restTask := make(map[string]*T, len(task.targets))

	for _, target := range task.targets {
		result, ok := up.GetTarget(target)
		if !ok || result == nil {
			missingTask[up.downRecorder(target)] = target
			continue
		}
		restTask[up.downRecorder(target)] = target
	}

	switch task.taskType {
	case upstreamUpdate:
		if task.source.Newest == nil {
			logrus.Errorf("%s get a nil Source", up.logPrefix())
			return nil
		}
		return up.UpstreamUpdate(task.source, mapToSlice[T](restTask), mapToSlice[T](missingTask))
	}
	logrus.Errorf("%s upstream task %s with error: creationType %d found, ignoring task", up.logPrefix(), up.upRecorder(task.source.Newest), task.taskType)
	return nil
}

func (up *upstreamer[S, T]) tryCreatingResource(task *creatingStream[S, T]) error {
	if task == nil {
		logrus.Errorf("%s retry a nil task", up.logPrefix())
		return nil
	}

	missingTask := make(map[string]*T)
	restTask := make(map[string]*T, len(task.targets))

	for _, target := range task.targets {
		result, ok := up.GetTarget(target)
		if !ok || result == nil {
			missingTask[up.downRecorder(target)] = target
			continue
		}
		restTask[up.downRecorder(target)] = target
	}

	switch task.taskType {
	case upstreamInflow:
		return up.UpstreamInflow(task.source, mapToSlice[T](restTask), mapToSlice[T](missingTask))
	case upstreamOutflow:
		return up.UpstreamInflow(task.source, mapToSlice[T](restTask), mapToSlice[T](missingTask))
	}
	logrus.Errorf("%s upstream task %s with error: creationType %d found, ignoring task", up.logPrefix(), up.upRecorder(task.source), task.taskType)
	return nil
}
