package coresync

import (
	"context"
	"fmt"
	"strings"
	"sync"

	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
)

type SourceNoFound struct {
	Source string
}

func (s *SourceNoFound) Error() string {
	return fmt.Sprintf("%s souece no found", s.Source)
}

type binder[S, T any] struct {
	upRecorder     RecordKey[S]
	downRecorder   RecordKey[T]
	upstreamBind   cmap.ConcurrentMap[*bindResource[S, T]]
	downstreamBind cmap.ConcurrentMap[*bindResource[T, S]]
}

func newBinder[S, T any](record recorder[S, T]) *binder[S, T] {
	return &binder[S, T]{
		upRecorder:     record.upstreamRecorder,
		downRecorder:   record.downstreamRecorder,
		upstreamBind:   cmap.New[*bindResource[S, T]](),
		downstreamBind: cmap.New[*bindResource[T, S]](),
	}
}

func (bin *binder[S, T]) CreateBind(source *S, target *T) error {
	if source == nil || target == nil {
		return fmt.Errorf("can not create a empty bind")
	}

	err := bin.bindToDownstream(source, target)
	if err != nil {
		return err
	}

	bin.bindToUpstream(source, target)
	return nil
}

func (bin *binder[S, T]) bindToUpstream(source *S, targets ...*T) {
	task, ok := bin.upstreamBind.Get(bin.upRecorder(source))

	if !ok {
		targetMap := cmap.New[*T]()
		for _, t := range targets {
			targetMap.Set(bin.downRecorder(t), t)
		}
		bin.upstreamBind.Set(bin.upRecorder(source), &bindResource[S, T]{source: source, targets: targetMap})
		return
	}

	for _, t := range targets {
		task.targets.Set(bin.downRecorder(t), t)
	}

	bin.upstreamBind.Set(bin.upRecorder(source), task)
}

func (bin *binder[S, T]) bindToDownstream(source *S, target *T) error {
	reverseTask, ok := bin.downstreamBind.Get(bin.downRecorder(target))

	if !ok {
		targetMap := cmap.New[*S]()
		targetMap.Set(bin.upRecorder(source), source)
		bin.downstreamBind.Set(bin.downRecorder(target), &bindResource[T, S]{source: target, targets: targetMap})
		return nil
	}

	_, ok = reverseTask.targets.Get(bin.upRecorder(source))
	if !ok {
		if len(reverseTask.targets.Items()) == 0 {
			logrus.Warningf("downstream %s should bind only one upstream, but get none", bin.downRecorder(target))
			reverseTask.targets.Set(bin.upRecorder(source), source)
			bin.downstreamBind.Set(bin.downRecorder(target), reverseTask)
			return nil
		}

		upstreamNames := make([]string, 0, len(reverseTask.targets.Items()))
		for name := range reverseTask.targets.Items() {
			upstreamNames = append(upstreamNames, name)
		}

		return fmt.Errorf("downstream %s want to bind upstream %s, but it already bind resource %s",
			bin.downRecorder(target), bin.upRecorder(source), strings.Join(upstreamNames, ", "))
	}
	return nil
}

func (bin *binder[S, T]) detachUpstreamBind(source *S, targets ...*T) {
	task, ok := bin.upstreamBind.Get(bin.upRecorder(source))
	if !ok {
		return
	}

	upstreamKey := bin.upRecorder(source)
	for _, target := range targets {
		task.targets.Remove(bin.downRecorder(target))
	}

	if len(task.targets.Items()) == 0 {
		bin.upstreamBind.Remove(upstreamKey)
		return
	}

	bin.upstreamBind.Set(upstreamKey, task)
}

func (bin *binder[S, T]) detachDownstreamBind(source *S, targets ...*T) {
	upstreamKey := bin.upRecorder(source)
	for _, target := range targets {
		downstreamKey := bin.downRecorder(target)
		reverseTask, ok := bin.downstreamBind.Get(downstreamKey)
		if !ok {
			continue
		}

		if _, ok = reverseTask.targets.Get(upstreamKey); ok {
			bin.downstreamBind.Remove(downstreamKey)
		}
	}
}

func (bin *binder[S, T]) deleteBind(source *S, targets ...*T) {
	bin.detachUpstreamBind(source, targets...)
	bin.detachDownstreamBind(source, targets...)
}

func (bin *binder[S, T]) deleteUpstream(source *S) {
	task, ok := bin.upstreamBind.Get(bin.upRecorder(source))
	if !ok {
		return
	}
	bin.upstreamBind.Remove(bin.upRecorder(source))
	bin.detachDownstreamBind(source, mapToSlice[T](task.targets.Items())...)
}

func (bin *binder[S, T]) deleteDownstream(target *T) {
	downstreamKey := bin.downRecorder(target)
	reverseTask, ok := bin.downstreamBind.Get(downstreamKey)
	if !ok {
		return
	}

	for _, source := range reverseTask.targets.Items() {
		bin.detachUpstreamBind(source, target)
	}
	bin.downstreamBind.Remove(downstreamKey)
}

func (bin *binder[S, T]) getUpstreamFromBindResource(task *bindResource[T, S]) (*S, error) {
	if task == nil {
		return nil, fmt.Errorf("updating stream is nil")
	}

	upstreamNames := make([]string, 0, len(task.targets.Items()))
	for name := range task.targets.Items() {
		upstreamNames = append(upstreamNames, name)
	}

	if len(upstreamNames) != 1 {
		return nil, fmt.Errorf("downstream %s bind upstream error: expected one upstream but get %s",
			bin.downRecorder(task.source), strings.Join(upstreamNames, ", "))
	}

	return task.targets.Items()[upstreamNames[0]], nil
}

func (bin *binder[S, T]) ListSyncUpstream() map[string]*S {
	tasks := bin.upstreamBind.Items()
	result := make(map[string]*S, len(tasks))
	for name, task := range tasks {
		result[name] = task.source
	}
	return result
}

func (bin *binder[S, T]) ListSyncDownstream() map[string]*T {
	tasks := bin.downstreamBind.Items()
	result := make(map[string]*T, len(tasks))
	for name, task := range tasks {
		result[name] = task.source
	}
	return result
}

func (bin *binder[S, T]) GetSyncDownstream(source *S) (map[string]*T, bool) {
	task, ok := bin.upstreamBind.Get(bin.upRecorder(source))
	if !ok {
		return nil, false
	}
	return task.targets.Items(), true
}

func (bin *binder[S, T]) GetSyncUpstream(target *T) (*S, bool) {
	task, ok := bin.downstreamBind.Get(bin.downRecorder(target))
	if !ok {
		return nil, false
	}

	source, err := bin.getUpstreamFromBindResource(task)
	if err != nil {
		logrus.Error(err)
		return nil, false
	}

	return source, true
}

type synchronizer[S, T any] struct {
	ObjectSearcher[S, T]
	name string
	once *sync.Once

	binder       *binder[S, T]
	upstreamer   *upstreamer[S, T]
	downstreamer *downstreamer[S, T]
}

type synchronizerFlux[S, T any] struct {
	upstreamInformers   []Informer[S]
	downstreamInformers []Informer[T]
	upstreamOperators   []UpstreamTrigger[S, T]
	downstreamOperators []DownstreamTrigger[S, T]
	getter              ObjectSearcher[S, T]
}

func newSynchronizer[S, T any](
	tp synchronizerFlux[S, T],
	record recorder[S, T],
	upOpt *option[S, T],
	downOpt *option[S, T],
) Synchronizer[S, T] {
	name := fmt.Sprintf("[Synchonizer/%T/%T]", *new(S), *new(T))
	upThroughput := upstreamFlux[S, T]{
		informers: tp.upstreamInformers,
		getter:    tp.getter,
		operators: tp.upstreamOperators,
	}

	downThroughput := downstreamThroughput[S, T]{
		informers: tp.downstreamInformers,
		getter:    tp.getter,
		operators: tp.downstreamOperators,
	}

	bind := newBinder[S, T](record)

	return &synchronizer[S, T]{
		name:         name,
		once:         &sync.Once{},
		binder:       bind,
		upstreamer:   newUpstreamer[S, T](upThroughput, bind, upOpt),
		downstreamer: newDownstreamer[S, T](downThroughput, bind, downOpt),
	}
}

func (s *synchronizer[S, T]) logPrefix() string {
	return s.name
}

func (s *synchronizer[S, T]) Run(ctx context.Context) {
	s.once.Do(func() {
		s.run(ctx)
	})
}

func (s *synchronizer[S, T]) run(ctx context.Context) {
	wg := sync.WaitGroup{}
	if s.upstreamer != nil {
		wg.Add(1)
		go func() {
			s.upstreamer.Run(ctx)
			wg.Done()
		}()
	}

	if s.downstreamer != nil {
		wg.Add(1)
		go func() {
			s.downstreamer.Run(ctx)
			wg.Done()
		}()
	}
	wg.Wait()
}

func (s *synchronizer[S, T]) CreateBind(source *S, target *T) error {
	err := s.binder.CreateBind(source, target)
	if err != nil {
		return fmt.Errorf("%s create bind error due to %v", s.logPrefix(), err)
	}
	return nil
}

func (s *synchronizer[S, T]) ListUpstream() map[string]*S {
	return s.binder.ListSyncUpstream()
}

func (s *synchronizer[S, T]) ListDownstream() map[string]*T {
	return s.binder.ListSyncDownstream()
}

func (s *synchronizer[S, T]) GetDownstreamFromUpstream(source *S) (map[string]*T, bool) {
	return s.binder.GetSyncDownstream(source)
}

func (s *synchronizer[S, T]) GetUpstreamFromDownstream(target *T) (*S, bool) {
	return s.binder.GetSyncUpstream(target)
}

func (s *synchronizer[S, T]) DeleteBind(source *S, targets ...*T) {
	s.binder.deleteBind(source, targets...)
}

func (s *synchronizer[S, T]) DeleteDownstream(target *T) {
	s.binder.deleteDownstream(target)
}

func (s *synchronizer[S, T]) DeleteUpstream(source *S) {
	s.binder.deleteUpstream(source)
}

func (s *synchronizer[S, T]) SetUpstreamStreamer(informers ...Informer[S]) error {
	if s.upstreamer == nil {
		return fmt.Errorf("can not set upstream infomrers to a downstreamer")
	}

	s.upstreamer.setStreamer(informers...)
	return nil
}

func (s *synchronizer[S, T]) SetUpstreamOperator(triggers ...UpstreamTrigger[S, T]) error {
	if s.upstreamer == nil {
		return fmt.Errorf("can not set upstream triggers to a downstreamer")
	}

	s.upstreamer.setOperator(triggers...)
	return nil
}

func (s *synchronizer[S, T]) SetDownstreamStreamer(informers ...Informer[T]) error {
	if s.downstreamer == nil {
		return fmt.Errorf("can not set downstreamer infomrers to a upstreamer")
	}

	s.downstreamer.setStreamer(informers...)
	return nil
}

func (s *synchronizer[S, T]) SetDownstreamOperator(triggers ...DownstreamTrigger[S, T]) error {
	if s.downstreamer == nil {
		return fmt.Errorf("can not set downstreamer triggers to a upstreamer")
	}

	s.downstreamer.setOperator(triggers...)
	return nil
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
