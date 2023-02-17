package coresync

import (
	"context"
	"fmt"
	"testing"
	"time"

	cmap "github.com/orcaman/concurrent-map"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

type testTargetInformerItf = Informer[testTargetResource]
type testSourceInformerItf = Informer[testSourceResource]
type testUpstreamOperatorItf = UpstreamTrigger[testSourceResource, testTargetResource]
type testDownstreamOperatorItf = DownstreamTrigger[testSourceResource, testTargetResource]

type testTargetResource struct {
	key   string
	value string
}

func (test *testTargetResource) DeepCopy() *testTargetResource {
	return &testTargetResource{
		key:   test.key,
		value: test.value,
	}
}

type testSourceResource struct {
	key   string
	value string
}

func (test *testSourceResource) DeepCopy() *testSourceResource {
	return &testSourceResource{
		key:   test.key,
		value: test.value,
	}
}

type resourceGetter struct {
	targetMap cmap.ConcurrentMap[*testTargetResource]
	sourceMap cmap.ConcurrentMap[*testSourceResource]
}

func newResourceGetter() *resourceGetter {
	return &resourceGetter{
		targetMap: cmap.New[*testTargetResource](),
		sourceMap: cmap.New[*testSourceResource](),
	}
}

func (g *resourceGetter) GetTarget(target *testTargetResource) (*testTargetResource, bool) {
	return g.targetMap.Get(target.key)
}

func (g *resourceGetter) ListTarget() []*testTargetResource {
	items := g.targetMap.Items()
	ret := make([]*testTargetResource, 0, len(items))
	for _, i := range items {
		ret = append(ret, i)
	}
	return ret
}

func (g *resourceGetter) GetSource(target *testSourceResource) (*testSourceResource, bool) {
	return g.sourceMap.Get(target.key)
}

func (g *resourceGetter) ListSource() []*testSourceResource {
	items := g.sourceMap.Items()
	ret := make([]*testSourceResource, 0, len(items))
	for _, i := range items {
		ret = append(ret, i)
	}
	return ret
}

type overtimeChecker struct {
	done chan struct{}
}

func newOvertimeChecker() *overtimeChecker {
	return &overtimeChecker{done: make(chan struct{})}
}

func (o *overtimeChecker) Done() {
	o.done <- struct{}{}
}

func (o *overtimeChecker) WaitUntil(timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	select {
	case <-o.done:
		return nil
	case <-timer.C:
		return fmt.Errorf("wait timeout")
	}
}

type testInformer[T any] struct {
	creationChannel CreationStreamChannel[T]
	updatingChannel UpdatingStreamChannel[T]
	deletionChannel CreationStreamChannel[T]
}

func (in *testInformer[T]) InflowChannel() CreationStreamChannel[T] {
	return in.creationChannel
}

func (in *testInformer[T]) UpdateChannel() UpdatingStreamChannel[T] {
	return in.updatingChannel
}

func (in *testInformer[T]) OutflowChannel() CreationStreamChannel[T] {
	return in.deletionChannel
}

func newTestInformer[T any]() *testInformer[T] {
	return &testInformer[T]{
		creationChannel: make(CreationStreamChannel[T]),
		updatingChannel: make(UpdatingStreamChannel[T]),
		deletionChannel: make(CreationStreamChannel[T]),
	}
}

func TestBinder(t *testing.T) {
	targetRecorder := func(res *testTargetResource) string {
		return res.key
	}

	sourceRecorder := func(res *testSourceResource) string {
		return res.key
	}
	bind := newBinder[testSourceResource, testTargetResource](recorder[testSourceResource, testTargetResource]{
		upstreamRecorder:   sourceRecorder,
		downstreamRecorder: targetRecorder,
	})

	sources := make([]*testSourceResource, 10)
	for i := range sources {
		sources[i] = &testSourceResource{key: fmt.Sprintf("sourceTest%d", i+1)}
	}
	targets := make([]*testTargetResource, 10)
	for i := range targets {
		targets[i] = &testTargetResource{key: fmt.Sprintf("targetTest%d", i+1)}
	}

	convey.Convey("test binder", t, func() {
		convey.Convey("test create bind", func() {
			err := bind.CreateBind(sources[0], targets[0])
			convey.So(err, convey.ShouldBeNil)
			targetMap, ok := bind.GetSyncDownstream(sources[0])
			ts := mapToSlice[testTargetResource](targetMap)
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(len(ts) == 1, convey.ShouldBeTrue)
			convey.So(ts[0].key == "targetTest1", convey.ShouldBeTrue)

			source, ok := bind.GetSyncUpstream(targets[0])
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(source.key == sources[0].key, convey.ShouldBeTrue)

			err = bind.CreateBind(sources[1], ts[0])
			convey.So(err != nil, convey.ShouldBeTrue)

			_, ok = bind.GetSyncDownstream(sources[1])
			convey.So(ok, convey.ShouldBeFalse)

			err = bind.CreateBind(sources[0], targets[1])
			convey.So(err, convey.ShouldBeNil)
			targetMap, ok = bind.GetSyncDownstream(sources[0])
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(len(targetMap) == 2, convey.ShouldBeTrue)
			_, ok = targetMap[targets[1].key]
			convey.So(ok, convey.ShouldBeTrue)

			listSource := bind.ListSyncUpstream()
			convey.So(len(listSource) == 1, convey.ShouldBeTrue)
			listTargets := bind.ListSyncDownstream()
			convey.So(len(listTargets) == 2, convey.ShouldBeTrue)
		})

		convey.Convey("test delete bind", func() {
			bind.DeleteBind(sources[0], targets[2])
			targetMap, ok := bind.GetSyncDownstream(sources[0])
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(len(targetMap) == 2, convey.ShouldBeTrue)
			_, ok = targetMap[targets[1].key]
			convey.So(ok, convey.ShouldBeTrue)

			bind.DeleteBind(sources[0], targets[1])
			targetMap, ok = bind.GetSyncDownstream(sources[0])
			ts := mapToSlice[testTargetResource](targetMap)
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(len(ts) == 1, convey.ShouldBeTrue)
			convey.So(ts[0].key == "targetTest1", convey.ShouldBeTrue)

			bind.DeleteBind(sources[0], targets[0])
			targetMap, ok = bind.GetSyncDownstream(sources[0])
			convey.So(ok, convey.ShouldBeFalse)

			err := bind.CreateBind(sources[0], targets[0])
			convey.So(err, convey.ShouldBeNil)

			bind.deleteUpstream(sources[0])
			_, ok = bind.GetSyncUpstream(targets[0])
			convey.So(ok, convey.ShouldBeFalse)
			_, ok = bind.GetSyncDownstream(sources[0])
			convey.So(ok, convey.ShouldBeFalse)

			err = bind.CreateBind(sources[0], targets[0])
			convey.So(err, convey.ShouldBeNil)

			bind.deleteDownstream(targets[0])
			_, ok = bind.GetSyncDownstream(sources[0])
			convey.So(ok, convey.ShouldBeFalse)
			_, ok = bind.GetSyncUpstream(targets[0])
			convey.So(ok, convey.ShouldBeFalse)
		})
	})
}

func TestBilateralSynchronizer(t *testing.T) {
	sourceInformer := newTestInformer[testSourceResource]()
	targetInformer := newTestInformer[testTargetResource]()

	targetRecorder := func(res *testTargetResource) string {
		return res.key
	}

	sourceRecorder := func(res *testSourceResource) string {
		return res.key
	}

	sources := make([]*testSourceResource, 10)
	for i := range sources {
		sources[i] = &testSourceResource{key: fmt.Sprintf("sourceTest%d", i+1)}
	}
	targets := make([]*testTargetResource, 10)
	for i := range targets {
		targets[i] = &testTargetResource{key: fmt.Sprintf("targetTest%d", i+1)}
	}

	getter := newResourceGetter()
	as := assert.New(t)
	minimumInterval := 200 * time.Millisecond
	maximumInterval := 400 * time.Millisecond

	b := builder[testSourceResource, testTargetResource]{}
	convey.Convey("test Upstream synchronizer core function", t, func() {
		sourceOperator := &testUpstreamOperator{}
		targetOperator := &testDownstreamOperator{}
		syn, err := b.SetRecorder(sourceRecorder, targetRecorder).
			SetSearcher(getter).
			SetBilateralStreamer([]testSourceInformerItf{sourceInformer}, []testTargetInformerItf{targetInformer}).
			SetBilateralOperator([]testUpstreamOperatorItf{sourceOperator}, []testDownstreamOperatorItf{targetOperator}).
			CreateSynchronizer(
				WithUpstreamMinimumUpdateInterval[testSourceResource, testTargetResource](minimumInterval),
				WithUpstreamMaximumUpdateInterval[testSourceResource, testTargetResource](maximumInterval),
				WithDownstreamMinimumUpdateInterval[testSourceResource, testTargetResource](minimumInterval),
				WithDownstreamMaximumUpdateInterval[testSourceResource, testTargetResource](maximumInterval),
			)

		convey.So(err, convey.ShouldBeNil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go syn.Run(ctx)

		err = syn.CreateBind(sources[0], targets[0])
		convey.So(err, convey.ShouldBeNil)
		err = syn.CreateBind(sources[0], targets[1])
		convey.So(err, convey.ShouldBeNil)
		err = syn.CreateBind(sources[0], targets[2])
		convey.So(err, convey.ShouldBeNil)

		getter.sourceMap.Set(sourceRecorder(sources[0]), sources[0])
		getter.targetMap.Set(targetRecorder(targets[1]), targets[1])
		getter.targetMap.Set(targetRecorder(targets[0]), targets[0])

		convey.Convey("test create Upstream synchronizer", func() {
			checker := newOvertimeChecker()
			tested := false
			sourceOperator.testUpstreamInflow = func(source *testSourceResource, rest, missing []*testTargetResource) error {
				tested = true
				as.Equal("sourceTest1", source.key)
				as.Equal(2, len(rest))
				as.Equal(1, len(missing))
				checker.Done()
				return nil
			}
			sourceInformer.creationChannel <- sources[0]
			err = checker.WaitUntil(200 * time.Millisecond)
			convey.So(err, convey.ShouldBeNil)
			convey.So(tested, convey.ShouldBeTrue)
		})

		convey.Convey("test Upstream delete", func() {
			deleted := sources[0].DeepCopy()
			checker := newOvertimeChecker()
			tested := false
			sourceOperator.testUpstreamOutflow = func(source *testSourceResource, rest, missing []*testTargetResource) error {
				tested = true
				as.Equal(deleted.key, source.key)
				checker.Done()
				return nil
			}

			sourceInformer.deletionChannel <- deleted
			err = checker.WaitUntil(200 * time.Millisecond)
			convey.So(err, convey.ShouldBeNil)
			convey.So(tested, convey.ShouldBeTrue)

			time.Sleep(100 * time.Millisecond)
			_, ok := syn.GetSyncDownstream(sources[0])
			convey.So(ok, convey.ShouldBeFalse)
		})

		tested := false
		convey.Convey("test create downstream synchronizer", func() {
			checker := newOvertimeChecker()
			targetOperator.testDownstreamInflow = func(source *testSourceResource, inflow *testTargetResource, rest, missing []*testTargetResource) error {
				tested = true
				as.Equal("sourceTest1", source.key)
				as.Equal("targetTest1", inflow.key)
				as.Equal(1, len(rest))
				as.Equal(1, len(missing))
				checker.Done()
				return nil
			}
			targetInformer.creationChannel <- targets[0]
			err = checker.WaitUntil(200 * time.Millisecond)
			convey.So(err, convey.ShouldBeNil)
			convey.So(tested, convey.ShouldBeTrue)
		})

		tested = false
		convey.Convey("test downstream delete", func() {
			deleted := targets[0].DeepCopy()
			checker := newOvertimeChecker()
			targetOperator.testDownstreamOutflow = func(source *testSourceResource, outflow *testTargetResource, rest, missing []*testTargetResource) error {
				tested = true
				as.Equal("sourceTest1", source.key)
				as.Equal(deleted.key, outflow.key)
				as.Equal(1, len(rest))
				as.Equal(1, len(missing))
				checker.Done()
				return nil
			}
			targetInformer.deletionChannel <- deleted
			err = checker.WaitUntil(200 * time.Millisecond)
			convey.So(err, convey.ShouldBeNil)

			time.Sleep(100 * time.Millisecond)
			targetMap, ok := syn.GetSyncDownstream(sources[0])
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(len(targetMap) == 2, convey.ShouldBeTrue)
			_, ok = targetMap[deleted.key]
			convey.So(ok, convey.ShouldBeFalse)

			_, ok = syn.GetSyncUpstream(deleted)
			convey.So(ok, convey.ShouldBeFalse)
			convey.So(tested, convey.ShouldBeTrue)
		})
	})
}
