package coresync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

type testDownstreamOperator struct {
	testDownstreamInflow  func(source *testSourceResource, inflow *testTargetResource, rest, missing []*testTargetResource) error
	testDownstreamUpdate  func(source *testSourceResource, update *ResourceUpdater[testTargetResource], rest, missing []*testTargetResource) error
	testDownstreamOutflow func(source *testSourceResource, inflow *testTargetResource, rest, missing []*testTargetResource) error
}

func (t *testDownstreamOperator) DownstreamInflow(source *testSourceResource, inflow *testTargetResource, rest, missing []*testTargetResource) error {
	return t.testDownstreamInflow(source, inflow, rest, missing)
}

func (t *testDownstreamOperator) DownstreamUpdate(source *testSourceResource, update *ResourceUpdater[testTargetResource], rest, missing []*testTargetResource) error {
	return t.testDownstreamUpdate(source, update, rest, missing)
}

func (t *testDownstreamOperator) DownstreamOutflow(source *testSourceResource, outflow *testTargetResource, rest, missing []*testTargetResource) error {
	return t.testDownstreamOutflow(source, outflow, rest, missing)
}

func TestDownstreamNormalFunc(t *testing.T) {
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
	tested := false

	b := builder[testSourceResource, testTargetResource]{}
	convey.Convey("test downstream synchronizer core function", t, func() {
		operator := &testDownstreamOperator{}
		syn, err := b.SetRecorder(sourceRecorder, targetRecorder).
			SetSearcher(getter).
			SetDownstreamStreamer(targetInformer).
			SetDownstreamOperator(operator).
			CreateSynchronizer(
				WithDownstreamMinimumUpdateInterval[testSourceResource, testTargetResource](minimumInterval),
				WithDownstreamMaximumUpdateInterval[testSourceResource, testTargetResource](maximumInterval),
			)

		convey.So(err, convey.ShouldBeNil)

		ctx, cancel := context.WithCancel(context.Background())
		go syn.Run(ctx)

		err = syn.CreateBind(sources[0], targets[0])
		convey.So(err, convey.ShouldBeNil)
		err = syn.CreateBind(sources[0], targets[1])
		convey.So(err, convey.ShouldBeNil)
		err = syn.CreateBind(sources[0], targets[2])
		convey.So(err, convey.ShouldBeNil)

		getter.sourceMap.Set(sourceRecorder(sources[0]), sources[0])
		getter.targetMap.Set(targetRecorder(targets[1]), targets[1])

		convey.Convey("test create downstream synchronizer", func() {
			checker := newOvertimeChecker()
			operator.testDownstreamInflow = func(source *testSourceResource, inflow *testTargetResource, rest, missing []*testTargetResource) error {
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
			operator.testDownstreamOutflow = func(source *testSourceResource, outflow *testTargetResource, rest, missing []*testTargetResource) error {
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
		cancel()
	})
}

func TestDownstreamUpdateFunc(t *testing.T) {
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
	startTime := time.Now()

	b := builder[testSourceResource, testTargetResource]{}
	tested := false
	convey.Convey("test downstream synchronizer core function", t, func() {
		operator := &testDownstreamOperator{}
		syn, err := b.SetRecorder(sourceRecorder, targetRecorder).
			SetSearcher(getter).
			SetDownstreamStreamer(targetInformer).
			SetDownstreamOperator(operator).
			CreateSynchronizer(
				WithDownstreamMinimumUpdateInterval[testSourceResource, testTargetResource](minimumInterval),
				WithDownstreamMaximumUpdateInterval[testSourceResource, testTargetResource](maximumInterval),
			)

		convey.So(err, convey.ShouldBeNil)

		ctx, cancel := context.WithCancel(context.Background())
		go syn.Run(ctx)
		//startTime := time.Now()

		err = syn.CreateBind(sources[0], targets[0])
		convey.So(err, convey.ShouldBeNil)
		err = syn.CreateBind(sources[0], targets[1])
		convey.So(err, convey.ShouldBeNil)
		err = syn.CreateBind(sources[0], targets[2])
		convey.So(err, convey.ShouldBeNil)

		getter.sourceMap.Set(sourceRecorder(sources[0]), sources[0])
		getter.targetMap.Set(targetRecorder(targets[1]), targets[1])

		convey.Convey("test downstream update", func() {
			updated := targets[0].DeepCopy()
			updated.value = "newTestValue"
			checker := newOvertimeChecker()
			lastestUpdate := time.Now()

			// test normal update
			operator.testDownstreamUpdate = func(source *testSourceResource,
				update *ResourceUpdater[testTargetResource],
				rest, missing []*testTargetResource) error {
				tested = true
				as.Equal("sourceTest1", source.key)
				as.Equal("targetTest1", update.ProbableOld.key)
				as.Equal("", update.ProbableOld.value)
				as.Equal("targetTest1", update.Newest.key)
				as.Equal("newTestValue", update.Newest.value)
				as.Equal(1, len(rest))
				as.Equal(1, len(missing))
				checker.Done()
				return nil
			}
			targetInformer.updatingChannel <- &ResourceUpdater[testTargetResource]{
				ProbableOld: targets[0],
				Newest:      updated,
			}
			lastestUpdate = time.Now()
			err = checker.WaitUntil(200 * time.Millisecond)
			convey.So(err, convey.ShouldBeNil)
			convey.So(tested, convey.ShouldBeTrue)

			// test update minimum interval
			newUpdated := updated.DeepCopy()
			newUpdated.value = "refreshedValue"
			tested = false
			operator.testDownstreamUpdate = func(source *testSourceResource,
				update *ResourceUpdater[testTargetResource],
				rest, missing []*testTargetResource) error {
				tested = true
				as.Equal("sourceTest1", source.key)
				as.Equal("targetTest1", update.ProbableOld.key)
				as.Equal("", update.ProbableOld.value)
				as.Equal("targetTest1", update.Newest.key)
				as.Equal(newUpdated.value, update.Newest.value)
				as.Equal(1, len(rest))
				as.Equal(1, len(missing))
				checker.Done()
				return nil
			}

			targetInformer.updatingChannel <- &ResourceUpdater[testTargetResource]{
				ProbableOld: targets[0],
				Newest:      newUpdated,
			}

			interval := time.Now().Sub(lastestUpdate)
			duration := minimumInterval - interval

			err = checker.WaitUntil(duration - 10*time.Millisecond)
			convey.So(err != nil, convey.ShouldBeTrue)

			err = checker.WaitUntil(20 * time.Millisecond)
			convey.So(err == nil, convey.ShouldBeTrue)
			convey.So(tested, convey.ShouldBeTrue)

			//test maximum interval
			tested = false
			operator.testDownstreamUpdate = func(source *testSourceResource,
				update *ResourceUpdater[testTargetResource],
				rest, missing []*testTargetResource) error {
				tested = true
				checker.Done()
				return nil
			}

			interval = time.Now().Sub(startTime)
			duration = maximumInterval - interval
			if duration < 0 {
				coeff := interval / maximumInterval
				duration = (coeff+1)*maximumInterval - interval
			}
			time.Sleep(duration - 10*time.Millisecond)

			err = checker.WaitUntil(duration + 10*time.Millisecond)
			convey.So(err == nil, convey.ShouldBeTrue)
			convey.So(tested, convey.ShouldBeTrue)

			//test normal update resource
			tested = false
			operator.testDownstreamUpdate = func(source *testSourceResource,
				update *ResourceUpdater[testTargetResource],
				rest, missing []*testTargetResource) error {
				tested = true
				as.Equal("sourceTest1", source.key)
				checker.Done()
				return nil
			}

			time.Sleep(100 * time.Millisecond)
			targetInformer.updatingChannel <- &ResourceUpdater[testTargetResource]{
				ProbableOld: targets[0],
				Newest:      updated,
			}

			err = checker.WaitUntil(100 * time.Millisecond)
			convey.So(err, convey.ShouldBeNil)
			convey.So(tested, convey.ShouldBeTrue)
		})
		cancel()
	})
}
