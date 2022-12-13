package coresync

import (
	"context"
	"fmt"
	"strings"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils/retry"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
)

type downstreamer[S, T any] struct {
	Getter[S, T]
	Informer[T]
	DownstreamOperator[S, T]

	name           string
	upstreamBind   cmap.ConcurrentMap[*bindResource[S, T]]
	downstreamBind cmap.ConcurrentMap[*bindResource[T, S]]
	upRecorder     RecordKey[S]
	downRecorder   RecordKey[T]
	creatingQueue  retry.RetryableQueue[creatingStream[S, T]]
	updatingQueue  retry.RetryableQueue[updatingStream[S, T]]
}

func newDownstreamer[S, T any](
	downInformer Informer[T],
	down DownstreamOperator[S, T],
	getter Getter[S, T],
	sRecorder RecordKey[S],
	tRecorder RecordKey[T],
	upstreamBind cmap.ConcurrentMap[*bindResource[S, T]],
	downstreamBind cmap.ConcurrentMap[*bindResource[T, S]],
	creatingRetry retry.RetryPolicy[creatingStream[S, T]],
	updatingRetry retry.RetryPolicy[updatingStream[S, T]]) *downstreamer[S, T] {
	name := fmt.Sprintf("[Synchonizer-%T-%T][Downstreamer]", *new(S), *new(T))
	sync := &downstreamer[S, T]{
		Informer:           downInformer,
		DownstreamOperator: down,
		Getter:             getter,
		name:               name,
		upstreamBind:       upstreamBind,
		downstreamBind:     downstreamBind,
		upRecorder:         sRecorder,
		downRecorder:       tRecorder,
	}

	creatingKeyFunc := func(task *creatingStream[S, T]) string {
		return fmt.Sprintf("%s-create-%d", tRecorder(task.incoming), task.taskType)
	}

	updatingKeyFunc := func(task *updatingStream[S, T]) string {
		return fmt.Sprintf("%s-update", tRecorder(task.incoming.Newest))
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

func (sync *downstreamer[S, T]) logPrefix() string {
	return sync.name
}

func (sync *downstreamer[S, T]) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case src, ok := <-sync.InflowChannel():
			if !ok {
				logrus.Errorf("synchronizer inflow channel has been closed unexpected")
				continue
			}
			err := sync.syncUpstreamWhenDownstreamCreating(src, downstreamInflow)
			if err != nil {
				logrus.Errorf("%s sync witeh error", sync.logPrefix())
			}

		case src, ok := <-sync.UpdateChannel():
			if !ok {
				logrus.Errorf("synchronizer update channel has been closed unexpected")
				continue
			}
			err := sync.syncUpstreamWhenUpstreamUpdating(src)
			if err != nil {
				logrus.Errorf("%s sync witeh error", sync.logPrefix())
			}

		case src, ok := <-sync.OutflowChannel():
			if !ok {
				logrus.Errorf("synchronizer outflow channel has been closed unexpected")
				continue
			}

			err := sync.syncUpstreamWhenDownstreamCreating(src, downstreamOutflow)
			if err != nil {
				logrus.Errorf("%s sync witeh error", sync.logPrefix())
			}
		}
	}
}

func (sync *downstreamer[S, T]) syncUpstreamWhenDownstreamCreating(target *T, taskType creationType) error {
	if target == nil {
		return fmt.Errorf("%s sync a empty Source", sync.logPrefix())
	}

	task, ok := sync.downstreamBind.Get(sync.downRecorder(target))
	if !ok {
		return nil
	}

	switch taskType {
	case downstreamInflow, downstreamOutflow:
		break
	default:
		return fmt.Errorf("creating type error, expected downstreamInflow and downstreamOutflow")
	}

	source, err := sync.getUpstreamFromBindResource(task)
	if err != nil {
		return err
	}

	upTask, ok := sync.upstreamBind.Get(sync.upRecorder(source))
	if !ok {
		return fmt.Errorf("can not find upstream task from a downstream task")
	}

	newTask := &creatingStream[S, T]{source: source, incoming: target, targets: upTask.targets.Items(), taskType: taskType}
	err = sync.creatingQueue.EnqueueTask(newTask)
	if err != nil {
		return fmt.Errorf("can not add task due to %v", err)
	}

	return nil
}

func (sync *downstreamer[S, T]) syncUpstreamWhenUpstreamUpdating(updated *ResourceUpdater[T]) error {
	if updated == nil || updated.Newest == nil {
		return fmt.Errorf("%s sync a empty Source", sync.logPrefix())
	}

	task, ok := sync.downstreamBind.Get(sync.downRecorder(updated.Newest))
	if !ok {
		return nil
	}

	source, err := sync.getUpstreamFromBindResource(task)
	if err != nil {
		return err
	}

	upTask, ok := sync.upstreamBind.Get(sync.upRecorder(source))
	if !ok {
		return fmt.Errorf("can not find upstream task from a downstream task")
	}

	newTask := &updatingStream[S, T]{source: &ResourceUpdater[S]{Newest: source}, incoming: updated,
		targets: upTask.targets.Items(), taskType: downstreamUpdate}
	err = sync.updatingQueue.EnqueueTask(newTask)
	if err != nil {
		return fmt.Errorf("can not add task due to %v", err)
	}

	return nil
}

func (sync *downstreamer[S, T]) getUpstreamFromBindResource(task *bindResource[T, S]) (*S, error) {
	if task == nil {
		return nil, fmt.Errorf("updating stream is nil")
	}

	upstreamNames := make([]string, 0, len(task.targets.Items()))
	for name := range task.targets.Items() {
		upstreamNames = append(upstreamNames, name)
	}

	if len(upstreamNames) != 1 {
		return nil, fmt.Errorf("%s downstream %s bind upstream error: expected one upstream but get %s", sync.logPrefix(),
			sync.downRecorder(task.source), strings.Join(upstreamNames, ", "))
	}

	return task.targets.Items()[upstreamNames[0]], nil
}

func (sync *downstreamer[S, T]) tryCreatingResource(task *creatingStream[S, T]) error {
	if task == nil {
		logrus.Errorf("%s retry a nil task", sync.logPrefix())
		return nil
	}

	missingTask := make(map[string]*T)
	restTask := make(map[string]*T, len(task.targets))

	for _, target := range task.targets {
		result, ok := sync.GetTarget(target)
		if !ok || result == nil {
			missingTask[sync.downRecorder(target)] = target
			continue
		}
		restTask[sync.downRecorder(target)] = target
	}

	result, ok := sync.GetSource(task.source)
	if !ok {
		return &SourceNoFound{
			Source: sync.upRecorder(task.source),
		}
	}
	task.source = result

	delete(restTask, sync.downRecorder(task.incoming))
	delete(missingTask, sync.downRecorder(task.incoming))

	switch task.taskType {
	case downstreamInflow:
		return sync.DownstreamInflow(task.source, task.incoming, mapToSlice[T](restTask), mapToSlice[T](missingTask))
	case downstreamOutflow:
		return sync.DownstreamOutflow(task.source, task.incoming, mapToSlice[T](restTask), mapToSlice[T](missingTask))
	}

	logrus.Errorf("%s upstream task %s with error: creationType %d found, ignoring task", sync.logPrefix(), sync.upRecorder(task.source), task.taskType)
	return nil
}

func (sync *downstreamer[S, T]) tryUpdatingResource(task *updatingStream[S, T]) error {
	if task == nil {
		logrus.Errorf("%s retry a nil task", sync.logPrefix())
		return nil
	}

	missingTask := make(map[string]*T)
	restTask := make(map[string]*T, len(task.targets))

	for _, target := range task.targets {
		result, ok := sync.GetTarget(target)
		if !ok || result == nil {
			missingTask[sync.downRecorder(target)] = target
			continue
		}
		restTask[sync.downRecorder(target)] = target
	}

	if task.incoming.Newest == nil {
		logrus.Errorf("%s get a nil incoming target", sync.logPrefix())
		return nil
	}

	result, ok := sync.GetSource(task.source.Newest)
	if !ok || result == nil {
		return &SourceNoFound{
			Source: sync.upRecorder(task.source.Newest),
		}
	}

	delete(restTask, sync.downRecorder(task.incoming.Newest))
	delete(missingTask, sync.downRecorder(task.incoming.Newest))

	switch task.taskType {
	case downstreamUpdate:
		return sync.DownstreamUpdate(result, task.incoming, mapToSlice[T](restTask), mapToSlice[T](missingTask))
	}

	logrus.Errorf("%s upstream task %s with error: creationType %d found, ignoring task", sync.logPrefix(), sync.upRecorder(task.source.Newest), task.taskType)
	return nil
}
