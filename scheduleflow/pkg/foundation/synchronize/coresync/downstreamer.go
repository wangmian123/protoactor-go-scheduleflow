package coresync

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/LiuYuuChen/algorithms/queue"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils/retry"
	"github.com/sirupsen/logrus"
)

type downstreamer[S, T any] struct {
	ObjectSearcher[S, T]
	informers []Informer[T]
	operators []DownstreamTrigger[S, T]
	*binder[S, T]

	name          string
	initialOnce   *sync.Once
	creatingQueue retry.RetryableQueue[creatingStream[S, T]]
	updatingQueue retry.RetryableQueue[updatingStream[S, T]]

	constraintUpdateQueue queue.DelayingQueue[*ResourceUpdater[T]]
	maxUpdateInterval     time.Duration
	minUpdateInterval     time.Duration
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

	if tp.operators == nil || tp.informers == nil || tp.getter == nil {
		return nil
	}

	if opt == nil {
		opt = newOption[S, T]()
	}

	d := &downstreamer[S, T]{
		informers:             tp.informers,
		ObjectSearcher:        tp.getter,
		name:                  name,
		initialOnce:           &sync.Once{},
		operators:             tp.operators,
		binder:                bind,
		maxUpdateInterval:     opt.maximumDownstreamUpdateInterval,
		minUpdateInterval:     opt.minimumDownstreamUpdateInterval,
		constraintUpdateQueue: queue.NewDelayingQueue[*ResourceUpdater[T]](newConstraintUpdateKey[T](bind.downRecorder)),
	}
	d.creatingQueue = retry.New[creatingStream[S, T]](creatingKeyFunc, retry.NewRetryFunc[creatingStream[S, T]](d.tryCreatingResource), opt.downstreamCreationRetryPolicy)
	d.updatingQueue = retry.New[updatingStream[S, T]](updatingKeyFunc, retry.NewRetryFunc[updatingStream[S, T]](d.tryUpdatingResource), opt.downstreamUpdatingRetryPolicy)
	return d
}

func (down *downstreamer[S, T]) logPrefix() string {
	return down.name
}

func (down *downstreamer[S, T]) runUpdateQueue(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		default:
			res, err := down.constraintUpdateQueue.Pop()
			if err != nil {
				logrus.Errorf("%s pop update queue with error %v", down.logPrefix(), err)
				continue
			}

			err = down.syncUpstreamWhenUpstreamUpdating(res)
			if err != nil {
				logrus.Errorf("%s down with error", down.logPrefix())
			}
		}
	}
}

func (down *downstreamer[S, T]) Run(ctx context.Context) {
	down.initialOnce.Do(func() {
		wg := sync.WaitGroup{}
		wg.Add(4)
		go func() {
			down.runUpdateQueue(ctx)
			wg.Done()
		}()

		go func() {
			down.creatingQueue.Run(ctx)
			wg.Done()
		}()

		go func() {
			down.updatingQueue.Run(ctx)
			wg.Done()
		}()

		go func() {
			down.run(ctx)
			wg.Done()
		}()
		wg.Wait()
	})
}

func (down *downstreamer[S, T]) run(ctx context.Context) {
	ticker := time.NewTicker(down.maxUpdateInterval)
	inflowCases := make([]reflect.SelectCase, len(down.informers))
	updateCases := make([]reflect.SelectCase, len(down.informers))
	outflowCases := make([]reflect.SelectCase, len(down.informers))
	for i := range down.informers {
		inflowCases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(down.informers[i].InflowChannel()),
		}
		updateCases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(down.informers[i].UpdateChannel()),
		}
		outflowCases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(down.informers[i].OutflowChannel()),
		}
	}

	go down.runReceivingInflowChannel(ctx, inflowCases)
	go down.runReceivingUpdateChannel(ctx, updateCases)
	go down.runReceivingOutflowChannel(ctx, outflowCases)

	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			down.constraintUpdateQueue.Shutdown()
			return

		case <-ticker.C:
			go down.batchCreateUpdatingTask(ctx)
		}
	}
}

func (down *downstreamer[S, T]) runReceivingInflowChannel(ctx context.Context, inflows []reflect.SelectCase) {
	cases := make([]reflect.SelectCase, 0, len(inflows)+1)
	cases = append(cases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	})
	cases = append(cases, inflows...)

	for {
		chosen, value, ok := reflect.Select(cases)
		switch chosen {
		case 0:
			return
		default:
			if !ok {
				logrus.Errorf("synchronizer %s inflow channel %d has been closed unexpected", down.name, chosen)
				continue
			}
			src, ok := value.Interface().(*T)
			if !ok {
				logrus.Errorf("synchronizer %s inflow channel %d receive type %T expected *T", down.name, chosen, value.Interface())
				continue
			}

			err := down.syncUpstreamWhenDownstreamCreating(src, downstreamInflow)
			if err != nil {
				logrus.Errorf("%s down with error", down.logPrefix())
			}
		}
	}
}

func (down *downstreamer[S, T]) runReceivingUpdateChannel(ctx context.Context, updates []reflect.SelectCase) {
	cases := make([]reflect.SelectCase, 0, len(updates)+1)
	cases = append(cases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	})
	cases = append(cases, updates...)

	for {
		chosen, value, ok := reflect.Select(cases)
		switch chosen {
		case 0:
			return
		default:
			if !ok {
				logrus.Errorf("synchronizer %s update channel %d has been closed unexpected", down.name, chosen)
				continue
			}
			src, ok := value.Interface().(*ResourceUpdater[T])
			if !ok {
				logrus.Errorf("synchronizer %s update channel %d receive type %T expected *T", down.name, chosen, value.Interface())
				continue
			}
			down.onUpdateEvent(src)
		}
	}
}

func (down *downstreamer[S, T]) runReceivingOutflowChannel(ctx context.Context, outflow []reflect.SelectCase) {
	cases := make([]reflect.SelectCase, 0, len(outflow)+1)
	cases = append(cases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	})
	cases = append(cases, outflow...)

	for {
		chosen, value, ok := reflect.Select(cases)
		switch chosen {
		case 0:
			return
		default:
			if !ok {
				logrus.Errorf("synchronizer %s outflow channel %d has been closed unexpected", down.name, chosen)
				continue
			}

			src, ok := value.Interface().(*T)
			if !ok {
				logrus.Errorf("synchronizer %s outflow channel %d receive type %T expected *T", down.name, chosen, value.Interface())
				continue
			}

			err := down.syncUpstreamWhenDownstreamCreating(src, downstreamOutflow)
			if err != nil {
				logrus.Errorf("%s down with error", down.logPrefix())
			}
		}
	}
}

func (down *downstreamer[S, T]) batchCreateUpdatingTask(ctx context.Context) {
	newCtx, cancel := context.WithTimeout(ctx, down.maxUpdateInterval)
	defer cancel()
	query := &ResourceUpdater[T]{}
	for _, task := range down.downstreamBind.Items() {
		select {
		case <-newCtx.Done():
			logrus.Errorf("%s batch updating downstream tasks timeout", down.logPrefix())
			return
		default:
			break
		}

		query.Newest = task.source
		if _, ok := down.constraintUpdateQueue.Get(query); ok {
			continue
		}

		newest, ok := down.GetTarget(task.source)
		if !ok {
			logrus.Debugf("%s can not get %s when create maximum updating task", down.logPrefix(), down.downRecorder(task.source))
			continue
		}

		down.onUpdateEvent(&ResourceUpdater[T]{Newest: newest})
	}
}

func (down *downstreamer[S, T]) onUpdateEvent(src *ResourceUpdater[T]) {
	if src == nil || src.Newest == nil {
		logrus.Errorf("%s down a empty Source", down.logPrefix())
		return
	}

	task, ok := down.downstreamBind.Get(down.downRecorder(src.Newest))
	if !ok {
		return
	}

	if down.minUpdateInterval == 0 || task.latestUpdate == nil {
		down.constraintUpdateQueue.Add(src)
		return
	}

	lastUpdateInterval := time.Now().Sub(*task.latestUpdate)
	if lastUpdateInterval < down.minUpdateInterval {
		interval := down.minUpdateInterval - lastUpdateInterval
		down.constraintUpdateQueue.AddAfter(src, interval)
		return
	}

	down.constraintUpdateQueue.Add(src)
}

func (down *downstreamer[S, T]) syncUpstreamWhenDownstreamCreating(target *T, taskType creationType) error {
	if target == nil {
		return fmt.Errorf("%s down a empty Source", down.logPrefix())
	}

	task, ok := down.downstreamBind.Get(down.downRecorder(target))
	if !ok {
		return nil
	}

	switch taskType {
	case downstreamInflow, downstreamOutflow:
		break
	default:
		return fmt.Errorf("creating type error, expected downstreamInflow and downstreamOutflow")
	}

	source, err := down.getUpstreamFromBindResource(task)
	if err != nil {
		return err
	}

	upTask, ok := down.upstreamBind.Get(down.upRecorder(source))
	if !ok {
		return fmt.Errorf("can not find upstream task from a downstream task")
	}

	newTask := &creatingStream[S, T]{source: source, incoming: target, targets: upTask.targets.Items(), taskType: taskType}
	unfinishedOperators := make(map[int]DownstreamTrigger[S, T], len(down.operators))
	for i := range down.operators {
		unfinishedOperators[i] = down.operators[i]
	}

	taskFunc := func(task *creatingStream[S, T]) error {
		runTaskFunc := func(ope DownstreamTrigger[S, T]) error {
			return down.tryCreatingResourceWithOperator(task, ope)
		}
		return down.retryUntilAllDone(runTaskFunc, unfinishedOperators)
	}
	err = down.creatingQueue.EnqueueTask(newTask, retry.WithRetryFunc[creatingStream[S, T]](taskFunc))
	if err != nil {
		return fmt.Errorf("can not add task due to %v", err)
	}

	return nil
}

func (down *downstreamer[S, T]) syncUpstreamWhenUpstreamUpdating(updated *ResourceUpdater[T]) error {
	if updated == nil || updated.Newest == nil {
		return fmt.Errorf("%s down a empty Source", down.logPrefix())
	}

	task, ok := down.downstreamBind.Get(down.downRecorder(updated.Newest))
	if !ok {
		return nil
	}

	source, err := down.getUpstreamFromBindResource(task)
	if err != nil {
		return err
	}

	upTask, ok := down.upstreamBind.Get(down.upRecorder(source))
	if !ok {
		return fmt.Errorf("can not find upstream task from a downstream task")
	}

	newTask := &updatingStream[S, T]{source: &ResourceUpdater[S]{Newest: source}, incoming: updated,
		targets: upTask.targets.Items(), taskType: downstreamUpdate}
	unfinishedOperators := make(map[int]DownstreamTrigger[S, T], len(down.operators))
	for i := range down.operators {
		unfinishedOperators[i] = down.operators[i]
	}

	taskFunc := func(task *updatingStream[S, T]) error {
		runTaskFunc := func(ope DownstreamTrigger[S, T]) error {
			return down.tryUpdatingResourceWithOperator(task, ope)
		}
		return down.retryUntilAllDone(runTaskFunc, unfinishedOperators)
	}
	err = down.updatingQueue.EnqueueTask(newTask, retry.WithRetryFunc[updatingStream[S, T]](taskFunc))
	if err != nil {
		return fmt.Errorf("can not add task due to %v", err)
	}

	now := time.Now()
	task.latestUpdate = &now
	down.downstreamBind.Set(down.downRecorder(task.source), task)

	return nil
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
		return nil, fmt.Errorf("%s downstream %s bind upstream error: expected one upstream but get %s", down.logPrefix(),
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

func (down *downstreamer[S, T]) tryCreatingResourceWithOperator(task *creatingStream[S, T], operator DownstreamTrigger[S, T]) error {
	if task == nil {
		logrus.Errorf("%s retry a nil task", down.logPrefix())
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
		err := operator.DownstreamInflow(task.source, task.incoming, mapToSlice[T](restTask), mapToSlice[T](missingTask))
		if err != nil {
			return err
		}
		return nil
	case downstreamOutflow:
		err := operator.DownstreamOutflow(task.source, task.incoming, mapToSlice[T](restTask), mapToSlice[T](missingTask))
		if err != nil {
			return err
		}
		down.deleteDownstream(task.incoming)
		return nil
	default:
		logrus.Errorf("%s downstream task %s with error: creationType %d found, ignoring task", down.logPrefix(), down.upRecorder(task.source), task.taskType)
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

func (down *downstreamer[S, T]) tryUpdatingResourceWithOperator(task *updatingStream[S, T], operator DownstreamTrigger[S, T]) error {
	if task == nil {
		logrus.Errorf("%s retry a nil task", down.logPrefix())
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
		logrus.Errorf("%s get a nil incoming target", down.logPrefix())
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
		err := operator.DownstreamUpdate(result, task.incoming, mapToSlice[T](restTask), mapToSlice[T](missingTask))
		if err != nil {
			return err
		}
		return nil
	default:
		logrus.Errorf("%s upstream task %s with error: creationType %d found, ignoring task", down.logPrefix(), down.upRecorder(task.source.Newest), task.taskType)
		return nil
	}
}

func (down *downstreamer[S, T]) retryUntilAllDone(runTask func(ope DownstreamTrigger[S, T]) error,
	unfinishedOperators map[int]DownstreamTrigger[S, T]) error {
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
	return fmt.Errorf("creating error, due to %s", strings.Trim(strings.Join(errorInfo, ", "), ", "))
}
