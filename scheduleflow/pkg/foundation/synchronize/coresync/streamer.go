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

type informerCases struct {
	inflowCases  []reflect.SelectCase
	updateCases  []reflect.SelectCase
	outflowCases []reflect.SelectCase
}

func newInformerCases[T any](informers []Informer[T]) *informerCases {
	cases := &informerCases{
		inflowCases:  make([]reflect.SelectCase, 0, len(informers)),
		updateCases:  make([]reflect.SelectCase, 0, len(informers)),
		outflowCases: make([]reflect.SelectCase, 0, len(informers)),
	}
	return addInformer[T](cases, informers...)
}

func addInformer[T any](cases *informerCases, informers ...Informer[T]) *informerCases {
	for i := range informers {
		inflowCase := reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(informers[i].InflowChannel()),
		}
		updateCase := reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(informers[i].UpdateChannel()),
		}
		outflowCase := reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(informers[i].OutflowChannel()),
		}

		cases.inflowCases = append(cases.inflowCases, inflowCase)
		cases.updateCases = append(cases.updateCases, updateCase)
		cases.outflowCases = append(cases.outflowCases, outflowCase)
	}

	return cases
}

type influxPlugin[S, T, U any] interface {
	inflowResource(*U) (*creatingStream[S, T], error)
	outflowResource(*U) (*creatingStream[S, T], error)
	updateResource(*ResourceUpdater[U]) (*updatingStream[S, T], error)
}

type operatorPlugin[S, T any] interface {
	yieldInflowOperator() func(task *creatingStream[S, T]) error
	yieldUpdateOperator() func(task *updatingStream[S, T]) error
	yieldOutflowOperator() func(task *creatingStream[S, T]) error
}

type fetcher[T any] interface {
	getUpdateBatch() []*T
	getNewestResource(*T) (*T, bool)
	getBindResource(*T) (queueUpdater, bool)
}

type streamer[S, T, U any] struct {
	name              string
	maxUpdateInterval time.Duration
	minUpdateInterval time.Duration
	initialOnce       *sync.Once
	informers         *informerCases

	creatingQueue         retry.RetryableQueue[creatingStream[S, T]]
	updatingQueue         retry.RetryableQueue[updatingStream[S, T]]
	constraintUpdateQueue queue.DelayingQueue[*ResourceUpdater[U]]

	influx   influxPlugin[S, T, U]
	fetcher  fetcher[U]
	operator operatorPlugin[S, T]
}

func (str *streamer[S, T, U]) logPrefix() string {
	return str.name
}

func (str *streamer[S, T, U]) runUpdateQueue(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		default:
			res, err := str.constraintUpdateQueue.Pop()
			if err != nil {
				logrus.Errorf("%s pop updateResource queue with error %v",
					str.logPrefix(), err)
				continue
			}

			updated, err := str.influx.updateResource(res)
			if err != nil {
				logrus.Errorf("%s str with error", str.logPrefix())
			}

			err = str.updatingQueue.EnqueueTask(updated,
				retry.WithRetryFunc[updatingStream[S, T]](str.operator.yieldUpdateOperator()))
			if err != nil {
				logrus.Errorf("can not add task due to %v", err)
			}
		}
	}
}

func (str *streamer[S, T, U]) Run(ctx context.Context) {
	str.initialOnce.Do(func() {
		wg := sync.WaitGroup{}
		wg.Add(4)
		go func() {
			str.runUpdateQueue(ctx)
			wg.Done()
		}()

		go func() {
			str.creatingQueue.Run(ctx)
			wg.Done()
		}()

		go func() {
			str.updatingQueue.Run(ctx)
			wg.Done()
		}()

		go func() {
			str.run(ctx)
			wg.Done()
		}()
		wg.Wait()
	})
}

func (str *streamer[S, T, U]) run(ctx context.Context) {
	ticker := time.NewTicker(str.maxUpdateInterval)
	go str.runReceivingInflowChannel(ctx)
	go str.runReceivingUpdateChannel(ctx)
	go str.runReceivingOutflowChannel(ctx)

	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			str.constraintUpdateQueue.Shutdown()
			return

		case <-ticker.C:
			go str.batchCreateUpdatingTask(ctx)
		}
	}
}

func (str *streamer[S, T, U]) runReceiving(ctx context.Context,
	cases []reflect.SelectCase, processor func(src any, channelNum int) error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			chosen, value, ok := reflect.Select(cases)
			if !ok {
				logrus.Errorf("synchronizer %s inflowResource channel %d"+
					" has been closed unexpected", str.name, chosen)
				continue
			}

			err := processor(value.Interface(), chosen)
			if err != nil {
				logrus.Errorf("%s streamer with error %v", str.logPrefix(), err)
			}
		}
	}
}

func (str *streamer[S, T, U]) runReceivingInflowChannel(ctx context.Context) {
	str.runReceiving(ctx, str.informers.inflowCases, func(value any, chosen int) error {
		src, ok := value.(*U)
		if !ok {
			return fmt.Errorf("synchronizer %s inflowResource channel %d "+
				"receive type %T expected *T", str.name, chosen, value)
		}

		task, err := str.influx.inflowResource(src)
		if err != nil {
			return fmt.Errorf("creating error %v", err)
		}

		if task == nil {
			return nil
		}

		err = str.creatingQueue.EnqueueTask(task,
			retry.WithRetryFunc[creatingStream[S, T]](str.operator.yieldInflowOperator()))
		if err != nil {
			return fmt.Errorf("can not add task due to %v", err)
		}
		return nil
	})
}

func (str *streamer[S, T, U]) runReceivingUpdateChannel(ctx context.Context) {
	str.runReceiving(ctx, str.informers.updateCases, func(value any, chosen int) error {
		src, ok := value.(*ResourceUpdater[U])
		if !ok {
			return fmt.Errorf("synchronizer %s updateResource "+
				"channel %d receive type %T expected *T", str.name, chosen, value)
		}

		if src == nil {
			return nil
		}

		str.onUpdateEvent(src)
		return nil
	})
}

func (str *streamer[S, T, U]) runReceivingOutflowChannel(ctx context.Context) {
	str.runReceiving(ctx, str.informers.outflowCases, func(value any, chosen int) error {
		src, ok := value.(*U)
		if !ok {
			return fmt.Errorf("synchronizer %s outflowResource channel"+
				" %d receive type %T expected *T", str.name, chosen, value)
		}

		task, err := str.influx.outflowResource(src)
		if err != nil {
			return fmt.Errorf("deleting error %v", err)
		}

		if task == nil {
			return nil
		}

		err = str.creatingQueue.EnqueueTask(task,
			retry.WithRetryFunc[creatingStream[S, T]](str.operator.yieldOutflowOperator()))
		if err != nil {
			return fmt.Errorf("can not add task due to %v", err)
		}

		return nil
	})
}

func (str *streamer[S, T, U]) onUpdateEvent(src *ResourceUpdater[U]) {
	if src == nil || src.Newest == nil {
		logrus.Errorf("%s on updateResource a empty Source", str.logPrefix())
		return
	}

	task, ok := str.fetcher.getBindResource(src.Newest)
	if !ok {
		return
	}

	if str.minUpdateInterval == 0 || task.getLatestUpdate() == nil {
		str.constraintUpdateQueue.Add(src)
		return
	}

	lastUpdateInterval := time.Now().Sub(*task.getLatestUpdate())
	if lastUpdateInterval < str.minUpdateInterval {
		interval := str.minUpdateInterval - lastUpdateInterval
		str.constraintUpdateQueue.AddAfter(src, interval)
		return
	}

	str.constraintUpdateQueue.Add(src)
}

func (str *streamer[S, T, U]) batchCreateUpdatingTask(ctx context.Context) {
	newCtx, cancel := context.WithTimeout(ctx, str.maxUpdateInterval)
	defer cancel()
	query := &ResourceUpdater[U]{}

	for _, task := range str.fetcher.getUpdateBatch() {
		select {
		case <-newCtx.Done():
			logrus.Errorf("%s batch updating strstream tasks timeout",
				str.logPrefix())
			return
		default:
			break
		}

		query.Newest = task
		if _, ok := str.constraintUpdateQueue.Get(query); ok {
			continue
		}

		newest, ok := str.fetcher.getNewestResource(task)
		if !ok {
			logrus.Debugf("%s can not get resource when batch updateResource",
				str.logPrefix())
			continue
		}

		str.onUpdateEvent(&ResourceUpdater[U]{Newest: newest})
	}
}

func yieldOperator[OPE, TASK any](operators []OPE,
	taskProcessor func(task *TASK, operator OPE) error) func(task *TASK) error {
	unfinishedOperators := make(map[int]OPE, len(operators))
	for i := range operators {
		unfinishedOperators[i] = operators[i]
	}

	return func(task *TASK) error {
		runTaskFunc := func(ope OPE) error {
			return taskProcessor(task, ope)
		}
		return retryUntilAllDone[OPE](runTaskFunc, unfinishedOperators)
	}
}

func retryUntilAllDone[OPE any](runTask func(ope OPE) error,
	unfinishedOperators map[int]OPE) error {
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
	return fmt.Errorf("creating error, due to %s", strings.Trim(strings.
		Join(errorInfo, ", "), ", "))
}
