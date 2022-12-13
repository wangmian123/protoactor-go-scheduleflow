package coresync

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils/retry"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
)

type SourceNoFound struct {
	Source string
}

func (s *SourceNoFound) Error() string {
	return fmt.Sprintf("%s souece no found", s.Source)
}

type synchronizer[S, T any] struct {
	Getter[S, T]

	name string
	once *sync.Once

	upRecorder   RecordKey[S]
	upstreamer   *upstreamer[S, T]
	upstreamBind cmap.ConcurrentMap[*bindResource[S, T]]

	downRecorder   RecordKey[T]
	downstreamer   *downstreamer[S, T]
	downstreamBind cmap.ConcurrentMap[*bindResource[T, S]]
}

func newSynchronizer[S, T any](
	upInformer Informer[S],
	downInformer Informer[T],
	up UpstreamOperator[S, T],
	down DownstreamOperator[S, T],
	getter Getter[S, T],
	sRecorder RecordKey[S],
	tRecorder RecordKey[T],
	creatingRetry retry.RetryPolicy[creatingStream[S, T]],
	updatingRetry retry.RetryPolicy[updatingStream[S, T]]) Synchronizer[S, T] {
	name := fmt.Sprintf("[Synchonizer-%T-%T]", *new(S), *new(T))
	syn := &synchronizer[S, T]{
		name: name,

		upstreamBind:   cmap.New[*bindResource[S, T]](),
		downstreamBind: cmap.New[*bindResource[T, S]](),
		upRecorder:     sRecorder,
		downRecorder:   tRecorder,
		once:           &sync.Once{},
	}

	if upInformer != nil && up != nil {
		syn.upstreamer = newUpstreamer[S, T](upInformer, up, getter, sRecorder, tRecorder, syn.upstreamBind, nil, nil)
	}

	if downInformer != nil && down != nil {
		syn.downstreamer = newDownstreamer[S, T](downInformer, down, getter, sRecorder, tRecorder, syn.upstreamBind, syn.downstreamBind, nil, nil)
	}

	return syn
}

func (syn *synchronizer[S, T]) logPrefix() string {
	return syn.name
}

func (syn *synchronizer[S, T]) Run(ctx context.Context) {
	syn.once.Do(func() {
		syn.run(ctx)
	})
}

func (syn *synchronizer[S, T]) run(ctx context.Context) {
	if syn.upstreamer != nil {
		go syn.upstreamer.run(ctx)
	}

	if syn.downstreamer != nil {
		go syn.downstreamer.run(ctx)
	}
}

func (syn *synchronizer[S, T]) CreateBind(source *S, targets ...*T) error {
	if len(targets) == 0 {
		return fmt.Errorf("can not create a empty bind")
	}

	task, ok := syn.upstreamBind.Get(syn.upRecorder(source))

	if !ok {
		targetMap := cmap.New[*T]()
		for _, t := range targets {
			targetMap.Set(syn.downRecorder(t), t)
		}
		syn.upstreamBind.Set(syn.upRecorder(source), &bindResource[S, T]{source: source, targets: targetMap})
		return nil
	}

	for _, t := range targets {
		task.targets.Set(syn.downRecorder(t), t)
	}

	syn.upstreamBind.Set(syn.upRecorder(source), task)

	for _, t := range targets {
		reverseTask, ok := syn.downstreamBind.Get(syn.downRecorder(t))

		if !ok {
			targetMap := cmap.New[*S]()
			targetMap.Set(syn.upRecorder(source), source)
			syn.downstreamBind.Set(syn.downRecorder(t), &bindResource[T, S]{source: t, targets: targetMap})
			return nil
		}

		_, ok = reverseTask.targets.Get(syn.upRecorder(source))
		if !ok {
			if len(reverseTask.targets.Items()) == 0 {
				logrus.Warningf("%s downstream %s should bind only one upstream, but get none", syn.logPrefix(), syn.downRecorder(t))
				reverseTask.targets.Set(syn.upRecorder(source), source)
				syn.downstreamBind.Set(syn.downRecorder(t), reverseTask)
				continue
			}

			upstreamNames := make([]string, 0, len(reverseTask.targets.Items()))
			for name := range reverseTask.targets.Items() {
				upstreamNames = append(upstreamNames, name)
			}

			return fmt.Errorf("%s downstream %s want to bind upstream %s, but it already bind resource %s",
				syn.logPrefix(), syn.downRecorder(t), syn.upRecorder(source), strings.Join(upstreamNames, ", "))
		}
	}

	return nil
}

func (syn *synchronizer[S, T]) getUpstreamFromBindResource(task *bindResource[T, S]) (*S, error) {
	if task == nil {
		return nil, fmt.Errorf("updating stream is nil")
	}

	upstreamNames := make([]string, 0, len(task.targets.Items()))
	for name := range task.targets.Items() {
		upstreamNames = append(upstreamNames, name)
	}

	if len(upstreamNames) != 1 {
		return nil, fmt.Errorf("%s downstream %s bind upstream error: expected one upstream but get %s", syn.logPrefix(),
			syn.downRecorder(task.source), strings.Join(upstreamNames, ", "))
	}

	return task.targets.Items()[upstreamNames[0]], nil
}

func (syn *synchronizer[S, T]) ListSyncUpstream() map[string]*S {
	tasks := syn.upstreamBind.Items()
	result := make(map[string]*S, len(tasks))
	for name, task := range tasks {
		result[name] = task.source
	}
	return result
}

func (syn *synchronizer[S, T]) ListSyncDownstream() map[string]*T {
	tasks := syn.downstreamBind.Items()
	result := make(map[string]*T, len(tasks))
	for name, task := range tasks {
		result[name] = task.source
	}
	return result
}

func (syn *synchronizer[S, T]) GetSyncDownstream(source *S) (map[string]*T, bool) {
	task, ok := syn.upstreamBind.Get(syn.upRecorder(source))
	if !ok {
		return nil, false
	}
	return task.targets.Items(), true
}

func (syn *synchronizer[S, T]) GetSyncUpstream(target *T) (*S, bool) {
	task, ok := syn.downstreamBind.Get(syn.downRecorder(target))
	if !ok {
		return nil, false
	}

	source, err := syn.getUpstreamFromBindResource(task)
	if err != nil {
		logrus.Error(err)
		return nil, false
	}

	return source, true
}

func (syn *synchronizer[S, T]) DeleteBind(source *S, targets ...*T) {
	task, ok := syn.upstreamBind.Get(syn.upRecorder(source))
	if !ok {
		return
	}

	upstreamKey := syn.upRecorder(source)
	for _, target := range targets {
		downstreamKey := syn.downRecorder(target)
		task.targets.Remove(downstreamKey)

		reverseTask, ok := syn.downstreamBind.Get(upstreamKey)
		if !ok {
			continue
		}
		if _, ok = reverseTask.targets.Get(upstreamKey); ok {
			task.targets.Remove(downstreamKey)
		}
	}

	if len(task.targets) == 0 {
		syn.upstreamBind.Remove(upstreamKey)
		return
	}
	syn.upstreamBind.Set(upstreamKey, task)
}

func mapToSlice[T any](inputs map[string]*T) []*T {
	if len(inputs) == 0 {
		return []*T{}
	}
	outputs := make([]*T, 0, len(inputs))
	for _, t := range inputs {
		outputs = append(outputs, t)
	}
	return outputs
}
