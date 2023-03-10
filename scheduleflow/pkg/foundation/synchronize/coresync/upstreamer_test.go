package coresync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

type testUpstreamOperator struct {
	testUpstreamInflow  func(source *testSourceResource, rest, missing []*testTargetResource) error
	testUpstreamUpdate  func(source *ResourceUpdater[testSourceResource], rest, missing []*testTargetResource) error
	testUpstreamOutflow func(source *testSourceResource, rest, missing []*testTargetResource) error
}

func (t *testUpstreamOperator) UpstreamInflow(source *testSourceResource, rest, missing []*testTargetResource) error {
	return t.testUpstreamInflow(source, rest, missing)
}

func (t *testUpstreamOperator) UpstreamUpdate(source *ResourceUpdater[testSourceResource], rest, missing []*testTargetResource) error {
	return t.testUpstreamUpdate(source, rest, missing)
}

func (t *testUpstreamOperator) UpstreamOutflow(source *testSourceResource, rest, missing []*testTargetResource) error {
	return t.testUpstreamOutflow(source, rest, missing)
}

func TestUpstreamAddFunc(t *testing.T) {
	sourceInformer := newTestInformer[testSourceResource]()

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

	b := builder[testSourceResource, testTargetResource]{}
	convey.Convey("test Upstream synchronizer core function", t, func() {
		operator := &testUpstreamOperator{}
		syn, err := b.SetRecorder(sourceRecorder, targetRecorder).
			SetSearcher(getter).
			SetUpstreamStreamer(sourceInformer).
			SetUpstreamOperator(operator).
			CreateSynchronizer(
				WithUpstreamMinimumUpdateInterval[testSourceResource, testTargetResource](200*time.Millisecond),
				WithUpstreamMaximumUpdateInterval[testSourceResource, testTargetResource](400*time.Millisecond),
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
			operator.testUpstreamInflow = func(source *testSourceResource, rest, missing []*testTargetResource) error {
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
	})
}

func TestUpstreamDeleteFunc(t *testing.T) {
	sourceInformer := newTestInformer[testSourceResource]()

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

	b := builder[testSourceResource, testTargetResource]{}
	convey.Convey("test Upstream synchronizer core function", t, func() {
		operator := &testUpstreamOperator{}
		syn, err := b.SetRecorder(sourceRecorder, targetRecorder).
			SetSearcher(getter).
			SetUpstreamStreamer(sourceInformer).
			SetUpstreamOperator(operator).
			CreateSynchronizer(
				WithUpstreamMinimumUpdateInterval[testSourceResource, testTargetResource](200*time.Millisecond),
				WithUpstreamMaximumUpdateInterval[testSourceResource, testTargetResource](400*time.Millisecond),
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

		convey.Convey("test Upstream delete", func() {
			deleted := sources[0].DeepCopy()
			checker := newOvertimeChecker()
			tested := false
			operator.testUpstreamOutflow = func(source *testSourceResource, rest, missing []*testTargetResource) error {
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
			_, ok := syn.GetDownstreamFromUpstream(sources[0])
			convey.So(ok, convey.ShouldBeFalse)
		})
	})
}

func TestUpstreamUpdateFunc(t *testing.T) {
	sourceInformer := newTestInformer[testSourceResource]()

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
	convey.Convey("test Upstream synchronizer core function", t, func() {
		operator := &testUpstreamOperator{}
		syn, err := b.SetRecorder(sourceRecorder, targetRecorder).
			SetSearcher(getter).
			SetUpstreamStreamer(sourceInformer).
			SetUpstreamOperator(operator).
			CreateSynchronizer(
				WithUpstreamMinimumUpdateInterval[testSourceResource, testTargetResource](minimumInterval),
				WithUpstreamMaximumUpdateInterval[testSourceResource, testTargetResource](maximumInterval),
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
		getter.targetMap.Set(targetRecorder(targets[0]), targets[0])

		convey.Convey("test Upstream updateResource", func() {
			updated := sources[0].DeepCopy()
			updated.value = "newTestValue"
			checker := newOvertimeChecker()
			lastUpdate := time.Now()

			tested := false
			// test normal updateResource
			operator.testUpstreamUpdate = func(source *ResourceUpdater[testSourceResource], rest, missing []*testTargetResource) error {
				tested = true
				as.Equal(updated.key, source.Newest.key)
				as.Equal(updated.value, source.Newest.value)
				as.Equal(2, len(rest))
				as.Equal(1, len(missing))
				checker.Done()
				return nil
			}
			sourceInformer.updatingChannel <- &ResourceUpdater[testSourceResource]{
				ProbableOld: sources[0],
				Newest:      updated,
			}
			lastUpdate = time.Now()
			err = checker.WaitUntil(minimumInterval)
			convey.So(err, convey.ShouldBeNil)
			convey.So(tested, convey.ShouldBeTrue)
			tested = false

			// test updateResource minimum interval
			newUpdated := updated.DeepCopy()
			newUpdated.value = "refreshedValue"

			operator.testUpstreamUpdate = func(source *ResourceUpdater[testSourceResource], rest, missing []*testTargetResource) error {
				tested = true
				as.Equal(newUpdated.key, source.Newest.key)
				as.Equal(newUpdated.value, source.Newest.value)
				as.Equal(2, len(rest))
				as.Equal(1, len(missing))
				checker.Done()
				return nil
			}

			sourceInformer.updatingChannel <- &ResourceUpdater[testSourceResource]{
				ProbableOld: sources[0],
				Newest:      newUpdated,
			}

			interval := time.Now().Sub(lastUpdate)
			duration := minimumInterval - interval

			err = checker.WaitUntil(duration - 10*time.Millisecond)
			convey.So(err != nil, convey.ShouldBeTrue)

			err = checker.WaitUntil(20 * time.Millisecond)
			convey.So(err == nil, convey.ShouldBeTrue)
			convey.So(tested, convey.ShouldBeTrue)

			//test maximum interval
			tested = false
			operator.testUpstreamUpdate = func(source *ResourceUpdater[testSourceResource], rest, missing []*testTargetResource) error {
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

			//test normal updateResource resource
			tested = false
			operator.testUpstreamUpdate = func(source *ResourceUpdater[testSourceResource], rest, missing []*testTargetResource) error {
				tested = true
				as.Equal(updated.key, source.Newest.key)
				checker.Done()
				return nil
			}

			time.Sleep(minimumInterval)
			sourceInformer.updatingChannel <- &ResourceUpdater[testSourceResource]{
				ProbableOld: sources[0],
				Newest:      updated,
			}

			err = checker.WaitUntil(100 * time.Millisecond)
			convey.So(err, convey.ShouldBeNil)
			convey.So(tested, convey.ShouldBeTrue)
		})
		cancel()
	})
}
