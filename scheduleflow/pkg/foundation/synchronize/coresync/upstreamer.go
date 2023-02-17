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

type upstreamer[S, T any] struct {
	ObjectSearcher[S, T]
	informers []Informer[S]
	operators []UpstreamTrigger[S, T]
	*binder[S, T]

	name          string
	initialOnce   *sync.Once
	creatingQueue retry.RetryableQueue[creatingStream[S, T]]
	updatingQueue retry.RetryableQueue[updatingStream[S, T]]

	constraintUpdateQueue queue.DelayingQueue[*ResourceUpdater[S]]
	maxUpdateInterval     time.Duration
	minUpdateInterval     time.Duration
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

	if tp.operators == nil || tp.informers == nil || tp.getter == nil {
		return nil
	}

	if opt == nil {
		opt = newOption[S, T]()
	}

	up := &upstreamer[S, T]{
		informers:             tp.informers,
		ObjectSearcher:        tp.getter,
		name:                  name,
		initialOnce:           &sync.Once{},
		operators:             tp.operators,
		binder:                bind,
		maxUpdateInterval:     opt.maximumUpstreamUpdateInterval,
		minUpdateInterval:     opt.minimumUpstreamUpdateInterval,
		constraintUpdateQueue: queue.NewDelayingQueue[*ResourceUpdater[S]](newConstraintUpdateKey[S](bind.upRecorder)),
	}
	up.creatingQueue = retry.New[creatingStream[S, T]](creatingKeyFunc, retry.NewRetryFunc[creatingStream[S, T]](up.tryCreatingResource), opt.upstreamCreationRetryPolicy)
	up.updatingQueue = retry.New[updatingStream[S, T]](updatingKeyFunc, retry.NewRetryFunc[updatingStream[S, T]](up.tryUpdatingResource), opt.upstreamUpdatingRetryPolicy)
	return up
}

func (up *upstreamer[S, T]) logPrefix() string {
	return up.name
}

func (up *upstreamer[S, T]) runUpdateQueue(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		default:
			res, err := up.constraintUpdateQueue.Pop()
			if err != nil {
				logrus.Debugf("%s pop update queue with error %v", up.logPrefix(), err)
				continue
			}

			err = up.syncDownstreamWhenUpstreamUpdating(res)
			if err != nil {
				logrus.Errorf("%s sync with error", up.logPrefix())
			}
		}
	}
}

func (up *upstreamer[S, T]) Run(ctx context.Context) {
	up.initialOnce.Do(func() {
		wg := sync.WaitGroup{}
		wg.Add(4)
		go func() {
			up.runUpdateQueue(ctx)
			wg.Done()
		}()

		go func() {
			up.creatingQueue.Run(ctx)
			wg.Done()
		}()

		go func() {
			up.updatingQueue.Run(ctx)
			wg.Done()
		}()

		go func() {
			up.run(ctx)
			wg.Done()
		}()
		wg.Wait()
	})
}

func (up *upstreamer[S, T]) run(ctx context.Context) {
	ticker := time.NewTicker(up.maxUpdateInterval)
	inflowCases := make([]reflect.SelectCase, len(up.informers))
	updateCases := make([]reflect.SelectCase, len(up.informers))
	outflowCases := make([]reflect.SelectCase, len(up.informers))
	for i := range up.informers {
		inflowCases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(up.informers[i].InflowChannel()),
		}
		updateCases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(up.informers[i].UpdateChannel()),
		}
		outflowCases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(up.informers[i].OutflowChannel()),
		}
	}

	go up.runReceivingInflowChannel(ctx, inflowCases)
	go up.runReceivingUpdateChannel(ctx, updateCases)
	go up.runReceivingOutflowChannel(ctx, outflowCases)

	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			up.constraintUpdateQueue.Shutdown()
			return

		case <-ticker.C:
			go up.batchCreateUpdatingTask(ctx)
		}
	}
}

func (up *upstreamer[S, T]) runReceivingInflowChannel(ctx context.Context, inflows []reflect.SelectCase) {
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
				logrus.Errorf("synchronizer %s inflow channel %d has been closed unexpected", up.name, chosen)
				continue
			}
			src, ok := value.Interface().(*S)
			if !ok {
				logrus.Errorf("synchronizer %s inflow channel %d receive type %T expected *T", up.name, chosen, value.Interface())
				continue
			}

			err := up.syncDownstreamWhenUpstreamCreating(src, upstreamInflow)
			if err != nil {
				logrus.Errorf("%s up with error", up.logPrefix())
			}
		}
	}
}

func (up *upstreamer[S, T]) runReceivingUpdateChannel(ctx context.Context, updates []reflect.SelectCase) {
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
				logrus.Errorf("synchronizer %s update channel %d has been closed unexpected", up.name, chosen)
				continue
			}
			src, ok := value.Interface().(*ResourceUpdater[S])
			if !ok {
				logrus.Errorf("synchronizer %s update channel %d receive type %T expected *T", up.name, chosen, value.Interface())
				continue
			}
			up.onUpdateEvent(src)
		}
	}
}

func (up *upstreamer[S, T]) runReceivingOutflowChannel(ctx context.Context, outflow []reflect.SelectCase) {
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
				logrus.Errorf("synchronizer %s outflow channel %d has been closed unexpected", up.name, chosen)
				continue
			}

			src, ok := value.Interface().(*S)
			if !ok {
				logrus.Errorf("synchronizer %s outflow channel %d receive type %T expected *T", up.name, chosen, value.Interface())
				continue
			}

			err := up.syncDownstreamWhenUpstreamCreating(src, upstreamOutflow)
			if err != nil {
				logrus.Errorf("%s up with error", up.logPrefix())
			}
		}
	}
}

func (up *upstreamer[S, T]) syncDownstreamWhenUpstreamCreating(source *S, taskType creationType) error {
	if source == nil {
		return fmt.Errorf("%s sync a empty Source", up.logPrefix())
	}

	task, ok := up.upstreamBind.Get(up.upRecorder(source))
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

	unfinishedOperators := make(map[int]UpstreamTrigger[S, T], len(up.operators))
	for i := range up.operators {
		unfinishedOperators[i] = up.operators[i]
	}

	taskFunc := func(task *creatingStream[S, T]) error {
		runTaskFunc := func(ope UpstreamTrigger[S, T]) error {
			return up.tryCreatingResourceWithOperator(task, ope)
		}
		return up.retryUntilAllDone(runTaskFunc, unfinishedOperators)
	}

	err := up.creatingQueue.EnqueueTask(newTask, retry.WithRetryFunc[creatingStream[S, T]](taskFunc))
	if err != nil {
		return fmt.Errorf("can not add task due to %v", err)
	}

	return nil
}

func (up *upstreamer[S, T]) syncDownstreamWhenUpstreamUpdating(source *ResourceUpdater[S]) error {
	if source == nil || source.Newest == nil {
		return fmt.Errorf("%s sync a empty Source", up.logPrefix())
	}

	task, ok := up.upstreamBind.Get(up.upRecorder(source.Newest))
	if !ok {
		return nil
	}

	newTask := &updatingStream[S, T]{source: source, targets: task.targets.Items(), taskType: upstreamUpdate}

	unfinishedOperators := make(map[int]UpstreamTrigger[S, T], len(up.operators))
	for i := range up.operators {
		unfinishedOperators[i] = up.operators[i]
	}

	taskFunc := func(task *updatingStream[S, T]) error {
		runTaskFunc := func(ope UpstreamTrigger[S, T]) error {
			return up.tryUpdatingResourceWithOperator(task, ope)
		}
		return up.retryUntilAllDone(runTaskFunc, unfinishedOperators)
	}

	err := up.updatingQueue.EnqueueTask(newTask, retry.WithRetryFunc[updatingStream[S, T]](taskFunc))
	if err != nil {
		return fmt.Errorf("can not add task due to %v", err)
	}

	now := time.Now()
	task.latestUpdate = &now
	up.upstreamBind.Set(up.upRecorder(task.source), task)

	return nil
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
		restTask[up.downRecorder(target)] = result
	}

	switch task.taskType {
	case upstreamUpdate:
		if task.source.Newest == nil {
			logrus.Errorf("%s get a nil Source", up.logPrefix())
			return nil
		}

		err := operator.UpstreamUpdate(task.source, mapToSlice[T](restTask), mapToSlice[T](missingTask))
		if err != nil {
			return err
		}
		return nil
	default:
		logrus.Errorf("%s upstream task %s with error: creationType %d found, ignoring task", up.logPrefix(), up.upRecorder(task.source.Newest), task.taskType)
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
		logrus.Errorf("%s upstream task %s with error: creationType %d found, ignoring task", up.logPrefix(), up.upRecorder(task.source), task.taskType)
		return nil
	}
}

func (up *upstreamer[S, T]) batchCreateUpdatingTask(ctx context.Context) {
	newCtx, _ := context.WithTimeout(ctx, up.maxUpdateInterval)
	query := &ResourceUpdater[S]{}
	for _, task := range up.upstreamBind.Items() {
		select {
		case <-newCtx.Done():
			logrus.Errorf("%s batch create upstream task timeout", up.logPrefix())
			return
		default:
			break
		}

		query.Newest = task.source
		if _, ok := up.constraintUpdateQueue.Get(query); ok {
			continue
		}

		newest, ok := up.GetSource(task.source)
		if !ok {
			logrus.Debugf("%s can not get %s when create maximum updating task", up.logPrefix(), up.upRecorder(task.source))
			continue
		}
		up.onUpdateEvent(&ResourceUpdater[S]{Newest: newest})
	}
}

func (up *upstreamer[S, T]) onUpdateEvent(src *ResourceUpdater[S]) {
	if src == nil || src.Newest == nil {
		logrus.Errorf("%s sync a empty Source", up.logPrefix())
		return
	}

	task, ok := up.upstreamBind.Get(up.upRecorder(src.Newest))
	if !ok {
		return
	}

	if up.minUpdateInterval == 0 || task.latestUpdate == nil {
		up.constraintUpdateQueue.Add(src)
		return
	}

	lastUpdateInterval := time.Now().Sub(*task.latestUpdate)
	if lastUpdateInterval < up.minUpdateInterval {
		interval := up.minUpdateInterval - lastUpdateInterval
		up.constraintUpdateQueue.AddAfter(src, interval)
		return
	}

	up.constraintUpdateQueue.Add(src)
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
