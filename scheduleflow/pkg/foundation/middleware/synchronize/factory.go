package synchronize

import (
	"fmt"
	"time"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/informer"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/kubernetes"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/synchronize/coresync"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"
	cmap "github.com/orcaman/concurrent-map"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const synchronizeInterval = 30 * time.Second

const pair = 2

type Option func(option *synchronizerOption)

type synchronizerOption struct {
	minimumSourceInterval time.Duration
	minimumTargetInterval time.Duration
	maximumSourceInterval time.Duration
	maximumTargetInterval time.Duration
}

func newOption() *synchronizerOption {
	return &synchronizerOption{
		minimumSourceInterval: synchronizeInterval,
		minimumTargetInterval: synchronizeInterval,
	}
}

func WithSyncSourceInterval(interval time.Duration) Option {
	return func(option *synchronizerOption) {
		option.minimumSourceInterval = interval
	}
}

func WithSyncTargetInterval(interval time.Duration) Option {
	return func(option *synchronizerOption) {
		option.minimumTargetInterval = interval
	}
}

type resourceSelector struct {
	cmap.ConcurrentMap[DynamicSynchronizer]
}

type clusterSelector struct {
	cmap.ConcurrentMap[resourceSelector]
}

type factory struct {
	informer.DynamicInformer
	kubernetes.Builder

	synchronizerMap clusterSelector
}

func NewFactory(dyInformer informer.DynamicInformer, k8sBuilder kubernetes.Builder) SynchronizerFactory {
	return &factory{
		DynamicInformer: dyInformer,
		Builder:         k8sBuilder,
		synchronizerMap: clusterSelector{cmap.New[resourceSelector]()},
	}
}

func (fac *factory) CreateSynchronizer(source, target SynchronizerObject, opts ...Option) (DynamicSynchronizer, error) {
	syncOpt := newOption()
	for _, opt := range opts {
		opt(syncOpt)
	}

	key := fmt.Sprintf("%s-%s", utils.FormActorKey(source.ClusterInformer), utils.FormActorKey(target.ClusterInformer))
	cluster, ok := fac.synchronizerMap.Get(key)
	if !ok {
		cluster = resourceSelector{ConcurrentMap: cmap.New[DynamicSynchronizer]()}
		fac.synchronizerMap.Set(key, cluster)
	}

	syn, ok := cluster.Get(formGVRKey(source.GVR, target.GVR))
	if !ok {
		sourceAPI := fac.GetResourceInterface(source.ClusterK8sAPI).Resource(source.GVR)
		targetAPI := fac.GetResourceInterface(target.ClusterK8sAPI).Resource(target.GVR)
		builder := newSynchronizerBuilder(sourceAPI, targetAPI, syncOpt.minimumSourceInterval, syncOpt.minimumTargetInterval)
		stores, err := fac.SetResourceHandler(
			informer.NewDynamicSubscribe(source.ClusterInformer, source.GVR),
			informer.NewDynamicSubscribe(target.ClusterInformer, target.GVR),
		)
		if err != nil {
			return nil, err
		}

		if len(stores) != pair {
			return nil, fmt.Errorf("stores length is not equal to SynchronizerObject number")
		}
		builder.SetSourceResourceStore(stores[0]).SetTargetResourceStore(stores[1])
		targetHandler := builder.GetTargetInformerHandler()
		sourceHandler := builder.GetSourceInformerHandler()
		stores[0].SetResourceHandler(informer.ConvertDynamicEventHandler(sourceHandler))
		stores[1].SetResourceHandler(informer.ConvertDynamicEventHandler(targetHandler))
		syn = builder.CreateSynchronizer()
		cluster.Set(formGVRKey(source.GVR, target.GVR), syn)
	}

	return syn, nil
}

type retryableFactory struct {
	informer.DynamicInformer
	kubernetes.Builder

	coreBuilder  coresync.Builder[DynamicLabelObject, DynamicLabelObject]
	cores        cmap.ConcurrentMap[coresync.Synchronizer[DynamicLabelObject, DynamicLabelObject]]
	synchronizer cmap.ConcurrentMap[*retryableSynchronizer]
}

func (fac *retryableFactory) CreateSynchronizer(source, target SynchronizerObject, opts ...Option) (DynamicSynchronizer, error) {
	syncOpt := newOption()
	for _, opt := range opts {
		opt(syncOpt)
	}

	if syn, ok := fac.synchronizer.Get(formSynchronizerKey(source, target)); ok {
		return syn, nil
	}

	core, ok := fac.cores.Get(formGVRKey(source.GVR, target.GVR))
	if !ok {
		var err error
		core, err = fac.createSynchronizerCore(source, target)
		if err != nil {
			return nil, err
		}
		fac.cores.Set(formGVRKey(source.GVR, target.GVR), core)
	}
	syn := newRetryableSynchronizer(fac.DynamicInformer, fac.Builder, source, target, core)
	fac.synchronizer.Set(formSynchronizerKey(source, target), syn)
	return syn, nil
}

func (fac *retryableFactory) createSynchronizerCore(source, target SynchronizerObject) (
	coresync.Synchronizer[DynamicLabelObject, DynamicLabelObject], error) {
	recordFunc := func(object *DynamicLabelObject) string {
		return formClusterResourceKey(object.Label[ClusterLabel], object.Resource.GetNamespace(),
			object.Resource.GetName())
	}

	sOperator := fac.GetResourceInterface(kubernetes.GetAPI(source.ClusterK8sAPI.Address)).Resource(source.GVR)
	tOperator := fac.GetResourceInterface(kubernetes.GetAPI(target.ClusterK8sAPI.Address)).Resource(target.GVR)
	searcher := newClusterSearcher(source.ClusterK8sAPI.Address, target.ClusterK8sAPI.Address, sOperator, tOperator)

	core, err := fac.coreBuilder.SetRecorder(recordFunc, recordFunc).SetSearcher(searcher).
		SetBilateralStreamer(nil, nil).SetBilateralOperator(nil, nil).CreateSynchronizer()
	if err != nil {
		return nil, fmt.Errorf("create retryable core synchronizer with error: %v", err)
	}

	return core, nil
}

func NewRetryableFactory(dyInformer informer.DynamicInformer, k8sBuilder kubernetes.Builder) SynchronizerFactory {
	return newRetryableFactory(dyInformer, k8sBuilder)
}

func newRetryableFactory(dyInformer informer.DynamicInformer, k8sBuilder kubernetes.Builder) *retryableFactory {
	return &retryableFactory{
		DynamicInformer: dyInformer,
		Builder:         k8sBuilder,
		coreBuilder:     coresync.NewBuilder[DynamicLabelObject, DynamicLabelObject](),
		cores:           cmap.New[coresync.Synchronizer[DynamicLabelObject, DynamicLabelObject]](),
		synchronizer:    cmap.New[*retryableSynchronizer](),
	}
}

func formGVRKey(sourceGVR, targetGVR schema.GroupVersionResource) string {
	return fmt.Sprintf("%s.%s.%s-%s.%s.%s", sourceGVR.Group, sourceGVR.Version, sourceGVR.Resource,
		targetGVR.Group, targetGVR.Version, targetGVR.Resource)
}

func formSynchronizerKey(source, target SynchronizerObject) string {
	return fmt.Sprintf("%s/%s.%s.%s-%s/%s.%s.%s", source.ClusterInformer.Address,
		source.GVR.Group, source.GVR.Version, source.GVR.Resource,
		target.ClusterInformer.Address, target.GVR.Group, target.GVR.Version, target.GVR.Resource)
}
