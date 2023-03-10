package coresync

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/LiuYuuChen/algorithms/queue"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils/retry"
	"github.com/sirupsen/logrus"
)

type upstreamer[S, T any] struct {
	ObjectSearcher[S, T]
	informers *informerCases
	operators []UpstreamTrigger[S, T]
	*binder[S, T]
	streamer *streamer[S, T, S]
}

type upstreamFlux[S, T any] struct {
	informers []Informer[S]
	operators []UpstreamTrigger[S, T]
	getter    ObjectSearcher[S, T]
}

func newUpstreamer[S, T any](
	tp upstreamFlux[S, T],
	bind *binder[S, T],
	opt *option[S, T],
) *upstreamer[S, T] {
	name := fmt.Sprintf("[Synchonizer/%T/%T][Upstreamer]", *new(S), *new(T))

	creatingKeyFunc := func(task *creatingStream[S, T]) string {
		return fmt.Sprintf("%s-create-%d", bind.upRecorder(task.source), task.taskType)
	}

	updatingKeyFunc := func(task *updatingStream[S, T]) string {
		return fmt.Sprintf("%s-update", bind.upRecorder(task.source.Newest))
	}

	if tp.getter == nil {
		return nil
	}

	if opt == nil {
		opt = newOption[S, T]()
	}

	up := &upstreamer[S, T]{
		informers:      newInformerCases[S](tp.informers),
		ObjectSearcher: tp.getter,
		operators:      tp.operators,
		binder:         bind,
	}

	queues := &streamer[S, T, S]{
		name:                  name,
		maxUpdateInterval:     opt.maximumUpstreamUpdateInterval,
		minUpdateInterval:     opt.minimumUpstreamUpdateInterval,
		initialOnce:           &sync.Once{},
		informers:             newInformerCases[S](tp.informers),
		constraintUpdateQueue: queue.NewDelayingQueue[*ResourceUpdater[S]](newConstraintUpdateKey[S](bind.upRecorder)),
		creatingQueue:         retry.New[creatingStream[S, T]](creatingKeyFunc, retry.NewRetryFunc[creatingStream[S, T]](up.tryCreatingResource), opt.upstreamCreationRetryPolicy),
		updatingQueue:         retry.New[updatingStream[S, T]](updatingKeyFunc, retry.NewRetryFunc[updatingStream[S, T]](up.tryUpdatingResource), opt.upstreamUpdatingRetryPolicy),
		influx:                up,
		fetcher:               up,
		operator:              up,
	}

	up.streamer = queues
	return up
}

func (up *upstreamer[S, T]) Run(ctx context.Context) {
	up.streamer.Run(ctx)
}

func (up *upstreamer[S, T]) setStreamer(informers ...Informer[S]) {
	addInformer[S](up.streamer.informers, informers...)
}

func (up *upstreamer[S, T]) setOperator(triggers ...UpstreamTrigger[S, T]) {
	up.operators = append(up.operators, triggers...)
}

func (up *upstreamer[S, T]) inflowResource(source *S) (*creatingStream[S, T], error) {
	return up.syncDownstreamWhenUpstreamCreating(source, upstreamInflow)
}

func (up *upstreamer[S, T]) updateResource(source *ResourceUpdater[S]) (*updatingStream[S, T], error) {
	return up.syncDownstreamWhenUpstreamUpdating(source)
}

func (up *upstreamer[S, T]) outflowResource(source *S) (*creatingStream[S, T], error) {
	return up.syncDownstreamWhenUpstreamCreating(source, upstreamOutflow)
}

func (up *upstreamer[S, T]) getNewestResource(source *S) (*S, bool) {
	return up.GetSource(source)
}

func (up *upstreamer[S, T]) getBindResource(source *S) (queueUpdater, bool) {
	return up.upstreamBind.Get(up.upRecorder(source))
}

func (up *upstreamer[S, T]) getUpdateBatch() []*S {
	items := make([]*S, 0, len(up.upstreamBind.Items()))
	for _, res := range up.upstreamBind.Items() {
		items = append(items, res.source)
	}
	return items
}

func (up *upstreamer[S, T]) yieldInflowOperator() func(task *creatingStream[S, T]) error {
	return yieldOperator[UpstreamTrigger[S, T], creatingStream[S, T]](
		up.operators, up.tryCreatingResourceWithOperator)
}

func (up *upstreamer[S, T]) yieldUpdateOperator() func(task *updatingStream[S, T]) error {
	return yieldOperator[UpstreamTrigger[S, T], updatingStream[S, T]](
		up.operators, up.tryUpdatingResourceWithOperator)
}

func (up *upstreamer[S, T]) yieldOutflowOperator() func(task *creatingStream[S, T]) error {
	return up.yieldInflowOperator()
}

func (up *upstreamer[S, T]) syncDownstreamWhenUpstreamCreating(source *S,
	taskType creationType) (*creatingStream[S, T], error) {
	if source == nil {
		return nil, fmt.Errorf("%s sync a empty Source", up.streamer.logPrefix())
	}

	task, ok := up.upstreamBind.Get(up.upRecorder(source))
	if !ok {
		return nil, nil
	}

	switch taskType {
	case upstreamInflow, upstreamOutflow:
		break
	default:
		return nil, fmt.Errorf("creating type error, expected upstreamInflow and upstreamOutflow")
	}

	return &creatingStream[S, T]{source: source, targets: task.targets.Items(), taskType: taskType}, nil
}

func (up *upstreamer[S, T]) syncDownstreamWhenUpstreamUpdating(source *ResourceUpdater[S]) (*updatingStream[S, T], error) {
	if source == nil || source.Newest == nil {
		return nil, fmt.Errorf("%s sync a empty Source", up.streamer.logPrefix())
	}

	task, ok := up.upstreamBind.Get(up.upRecorder(source.Newest))
	if !ok {
		return nil, nil
	}

	now := time.Now()
	task.latestUpdate = &now
	up.upstreamBind.Set(up.upRecorder(task.source), task)

	return &updatingStream[S, T]{source: source, targets: task.targets.Items(), taskType: upstreamUpdate}, nil
}

func (up *upstreamer[S, T]) tryUpdatingResource(task *updatingStream[S, T]) error {
	for _, operator := range up.operators {
		err := up.tryUpdatingResourceWithOperator(task, operator)
		if err != nil {
			return err
		}
	}
	return nil
}

func (up *upstreamer[S, T]) tryUpdatingResourceWithOperator(task *updatingStream[S, T], operator UpstreamTrigger[S, T]) error {
	if task == nil {
		logrus.Errorf("%s retry a nil task", up.streamer.logPrefix())
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
		restTask[up.downRecorder(target)] = result
	}

	switch task.taskType {
	case upstreamUpdate:
		if task.source.Newest == nil {
			logrus.Errorf("%s get a nil Source", up.streamer.logPrefix())
			return nil
		}

		err := operator.UpstreamUpdate(task.source, mapToSlice[T](restTask), mapToSlice[T](missingTask))
		if err != nil {
			return err
		}
		return nil
	default:
		logrus.Errorf("%s upstream task %s with error: creationType %d found, ignoring task",
			up.streamer.logPrefix(), up.upRecorder(task.source.Newest), task.taskType)
		return nil
	}
}

func (up *upstreamer[S, T]) tryCreatingResource(task *creatingStream[S, T]) error {
	for _, operator := range up.operators {
		err := up.tryCreatingResourceWithOperator(task, operator)
		if err != nil {
			return err
		}
	}
	return nil
}

func (up *upstreamer[S, T]) tryCreatingResourceWithOperator(task *creatingStream[S, T], operator UpstreamTrigger[S, T]) error {
	if task == nil {
		logrus.Errorf("%s retry a nil task", up.streamer.logPrefix())
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
		restTask[up.downRecorder(target)] = result
	}

	switch task.taskType {
	case upstreamInflow:
		err := operator.UpstreamInflow(task.source, mapToSlice[T](restTask), mapToSlice[T](missingTask))
		if err != nil {
			return err
		}
		return nil
	case upstreamOutflow:
		err := operator.UpstreamOutflow(task.source, mapToSlice[T](restTask), mapToSlice[T](missingTask))
		if err != nil {
			return err
		}
		up.deleteUpstream(task.source)
		return nil
	default:
		logrus.Errorf("%s upstream task %s with error: creationType %d found, ignoring task", up.streamer.logPrefix(), up.upRecorder(task.source), task.taskType)
		return nil
	}
}

func (up *upstreamer[S, T]) retryUntilAllDone(runTask func(ope UpstreamTrigger[S, T]) error,
	unfinishedOperators map[int]UpstreamTrigger[S, T]) error {
	var errors []error
	for i, ope := range unfinishedOperators {
		err := runTask(ope)
		if err == nil {
			delete(unfinishedOperators, i)
			continue
		}
		errors = append(errors, err)
	}

	if len(errors) == 0 {
		return nil
	}
	errorInfo := make([]string, len(errors))
	for i := range errors {
		errorInfo[i] = errors[i].Error()
	}
	return fmt.Errorf("run task error, due to %s", strings.Trim(strings.Join(errorInfo, ", "), ", "))
}
