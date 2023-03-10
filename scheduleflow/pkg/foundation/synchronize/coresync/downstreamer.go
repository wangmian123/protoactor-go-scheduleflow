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

type downstreamer[S, T any] struct {
	ObjectSearcher[S, T]
	operators []DownstreamTrigger[S, T]
	*binder[S, T]
	streamer *streamer[S, T, T]
}

type downstreamThroughput[S, T any] struct {
	informers []Informer[T]
	operators []DownstreamTrigger[S, T]
	getter    ObjectSearcher[S, T]
}

func newDownstreamer[S, T any](
	tp downstreamThroughput[S, T],
	bind *binder[S, T],
	opt *option[S, T],
) *downstreamer[S, T] {
	name := fmt.Sprintf("[Synchonizer/%T/%T][Downstreamer]", *new(S), *new(T))
	creatingKeyFunc := func(task *creatingStream[S, T]) string {
		return fmt.Sprintf("%s-create-%d", bind.downRecorder(task.incoming), task.taskType)
	}

	updatingKeyFunc := func(task *updatingStream[S, T]) string {
		return fmt.Sprintf("%s-update", bind.downRecorder(task.incoming.Newest))
	}

	if tp.getter == nil {
		return nil
	}

	if opt == nil {
		opt = newOption[S, T]()
	}

	d := &downstreamer[S, T]{
		ObjectSearcher: tp.getter,
		operators:      tp.operators,
		binder:         bind,
	}

	queues := &streamer[S, T, T]{
		name:                  name,
		maxUpdateInterval:     opt.maximumDownstreamUpdateInterval,
		minUpdateInterval:     opt.minimumDownstreamUpdateInterval,
		initialOnce:           &sync.Once{},
		informers:             newInformerCases[T](tp.informers),
		constraintUpdateQueue: queue.NewDelayingQueue[*ResourceUpdater[T]](newConstraintUpdateKey[T](bind.downRecorder)),
		creatingQueue:         retry.New[creatingStream[S, T]](creatingKeyFunc, retry.NewRetryFunc[creatingStream[S, T]](d.tryCreatingResource), opt.downstreamCreationRetryPolicy),
		updatingQueue:         retry.New[updatingStream[S, T]](updatingKeyFunc, retry.NewRetryFunc[updatingStream[S, T]](d.tryUpdatingResource), opt.downstreamUpdatingRetryPolicy),
		influx:                d,
		fetcher:               d,
		operator:              d,
	}
	d.streamer = queues
	return d
}

func (down *downstreamer[S, T]) Run(ctx context.Context) {
	down.streamer.Run(ctx)
}

func (down *downstreamer[S, T]) setStreamer(informers ...Informer[T]) {
	addInformer[T](down.streamer.informers, informers...)
}

func (down *downstreamer[S, T]) setOperator(triggers ...DownstreamTrigger[S, T]) {
	down.operators = append(down.operators, triggers...)
}

func (down *downstreamer[S, T]) inflowResource(target *T) (*creatingStream[S, T], error) {
	return down.syncUpstreamWhenDownstreamCreating(target, downstreamInflow)
}

func (down *downstreamer[S, T]) outflowResource(target *T) (*creatingStream[S, T], error) {
	return down.syncUpstreamWhenDownstreamCreating(target, downstreamOutflow)
}

func (down *downstreamer[S, T]) updateResource(target *ResourceUpdater[T]) (*updatingStream[S, T], error) {
	return down.syncUpstreamWhenUpstreamUpdating(target)
}

func (down *downstreamer[S, T]) getUpdateBatch() []*T {
	items := make([]*T, 0, len(down.downstreamBind.Items()))
	for _, res := range down.downstreamBind.Items() {
		items = append(items, res.source)
	}
	return items
}

func (down *downstreamer[S, T]) yieldInflowOperator() func(task *creatingStream[S, T]) error {
	return yieldOperator[DownstreamTrigger[S, T], creatingStream[S, T]](
		down.operators, down.tryCreatingResourceWithOperator)
}

func (down *downstreamer[S, T]) yieldUpdateOperator() func(task *updatingStream[S, T]) error {
	return yieldOperator[DownstreamTrigger[S, T], updatingStream[S, T]](
		down.operators, down.tryUpdatingResourceWithOperator)
}

func (down *downstreamer[S, T]) yieldOutflowOperator() func(task *creatingStream[S, T]) error {
	return down.yieldInflowOperator()
}

func (down *downstreamer[S, T]) getNewestResource(target *T) (*T, bool) {
	return down.GetTarget(target)
}

func (down *downstreamer[S, T]) getBindResource(target *T) (queueUpdater, bool) {
	return down.downstreamBind.Get(down.downRecorder(target))
}

func (down *downstreamer[S, T]) syncUpstreamWhenDownstreamCreating(target *T, taskType creationType) (*creatingStream[S, T], error) {
	if target == nil {
		return nil, fmt.Errorf("%s down a empty Source", down.streamer.logPrefix())
	}

	task, ok := down.downstreamBind.Get(down.downRecorder(target))
	if !ok {
		return nil, nil
	}

	switch taskType {
	case downstreamInflow, downstreamOutflow:
		break
	default:
		return nil, fmt.Errorf("creating type error, expected downstreamInflow and downstreamOutflow")
	}

	source, err := down.getUpstreamFromBindResource(task)
	if err != nil {
		return nil, err
	}

	upTask, ok := down.upstreamBind.Get(down.upRecorder(source))
	if !ok {
		return nil, fmt.Errorf("can not find upstream task from a downstream task")
	}

	return &creatingStream[S, T]{source: source, incoming: target, targets: upTask.targets.Items(), taskType: taskType}, nil
}

func (down *downstreamer[S, T]) syncUpstreamWhenUpstreamUpdating(updated *ResourceUpdater[T]) (*updatingStream[S, T], error) {
	if updated == nil || updated.Newest == nil {
		return nil, fmt.Errorf("%s down a empty Source", down.streamer.logPrefix())
	}

	task, ok := down.downstreamBind.Get(down.downRecorder(updated.Newest))
	if !ok {
		return nil, nil
	}

	source, err := down.getUpstreamFromBindResource(task)
	if err != nil {
		return nil, err
	}

	upTask, ok := down.upstreamBind.Get(down.upRecorder(source))
	if !ok {
		return nil, fmt.Errorf("can not find upstream task from a downstream task")
	}

	now := time.Now()
	task.latestUpdate = &now
	down.downstreamBind.Set(down.downRecorder(task.source), task)

	return &updatingStream[S, T]{source: &ResourceUpdater[S]{Newest: source}, incoming: updated,
		targets: upTask.targets.Items(), taskType: downstreamUpdate}, nil
}

func (down *downstreamer[S, T]) getUpstreamFromBindResource(task *bindResource[T, S]) (*S, error) {
	if task == nil {
		return nil, fmt.Errorf("updating stream is nil")
	}

	upstreamNames := make([]string, 0, len(task.targets.Items()))
	items := make([]*S, 0, len(task.targets.Items()))
	for name, item := range task.targets.Items() {
		upstreamNames = append(upstreamNames, name)
		items = append(items, item)
	}

	if len(upstreamNames) != 1 {
		return nil, fmt.Errorf("%s downstream %s bind upstream error: expected one upstream but get %s", down.streamer.logPrefix(),
			down.downRecorder(task.source), strings.Join(upstreamNames, ", "))
	}
	return items[0], nil
}

func (down *downstreamer[S, T]) tryCreatingResource(task *creatingStream[S, T]) error {
	for _, operator := range down.operators {
		err := down.tryCreatingResourceWithOperator(task, operator)
		if err != nil {
			return err
		}
	}
	return nil
}

func (down *downstreamer[S, T]) tryCreatingResourceWithOperator(
	task *creatingStream[S, T], operator DownstreamTrigger[S, T]) error {
	if task == nil {
		logrus.Errorf("%s retry a nil task", down.streamer.logPrefix())
		return nil
	}

	missingTask := make(map[string]*T)
	restTask := make(map[string]*T, len(task.targets))

	for _, target := range task.targets {
		result, ok := down.GetTarget(target)
		if !ok || result == nil {
			missingTask[down.downRecorder(target)] = target
			continue
		}
		restTask[down.downRecorder(target)] = result
	}

	result, ok := down.GetSource(task.source)
	if !ok {
		return &SourceNoFound{
			Source: down.upRecorder(task.source),
		}
	}
	task.source = result

	delete(restTask, down.downRecorder(task.incoming))
	delete(missingTask, down.downRecorder(task.incoming))

	switch task.taskType {
	case downstreamInflow:
		err := operator.DownstreamInflow(task.source, task.incoming,
			mapToSlice[T](restTask), mapToSlice[T](missingTask))
		if err != nil {
			return err
		}
		return nil
	case downstreamOutflow:
		err := operator.DownstreamOutflow(task.source, task.incoming,
			mapToSlice[T](restTask), mapToSlice[T](missingTask))
		if err != nil {
			return err
		}
		down.deleteDownstream(task.incoming)
		return nil
	default:
		logrus.Errorf("%s downstream task %s with error: creationType %d found, ignoring task",
			down.streamer.logPrefix(), down.upRecorder(task.source), task.taskType)
		return nil
	}
}

func (down *downstreamer[S, T]) tryUpdatingResource(task *updatingStream[S, T]) error {
	for _, operator := range down.operators {
		err := down.tryUpdatingResourceWithOperator(task, operator)
		if err != nil {
			return err
		}
	}
	return nil
}

func (down *downstreamer[S, T]) tryUpdatingResourceWithOperator(
	task *updatingStream[S, T], operator DownstreamTrigger[S, T]) error {
	if task == nil {
		logrus.Errorf("%s retry a nil task", down.streamer.logPrefix())
		return nil
	}

	missingTask := make(map[string]*T)
	restTask := make(map[string]*T, len(task.targets))

	for _, target := range task.targets {
		result, ok := down.GetTarget(target)
		if !ok || result == nil {
			missingTask[down.downRecorder(target)] = target
			continue
		}
		restTask[down.downRecorder(target)] = result
	}

	if task.incoming.Newest == nil {
		logrus.Errorf("%s get a nil incoming target",
			down.streamer.logPrefix())
		return nil
	}

	result, ok := down.GetSource(task.source.Newest)
	if !ok || result == nil {
		return &SourceNoFound{
			Source: down.upRecorder(task.source.Newest),
		}
	}

	delete(restTask, down.downRecorder(task.incoming.Newest))
	delete(missingTask, down.downRecorder(task.incoming.Newest))

	switch task.taskType {
	case downstreamUpdate:
		err := operator.DownstreamUpdate(result, task.incoming,
			mapToSlice[T](restTask), mapToSlice[T](missingTask))
		if err != nil {
			return err
		}
		return nil
	default:
		logrus.Errorf("%s upstream task %s with error: creationType %d found, ignoring task",
			down.streamer.logPrefix(), down.upRecorder(task.source.Newest), task.taskType)
		return nil
	}
}
