package synchronize

import (
	"fmt"
	"time"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/informer"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/kubernetes"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"
	cmap "github.com/orcaman/concurrent-map"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const synchronizeInterval = 30 * time.Second

const pair = 2

type Option func(option *synchronizerOption)

type synchronizerOption struct {
	updateSourceInterval time.Duration
	updateTargetInterval time.Duration
}

func newOption() *synchronizerOption {
	return &synchronizerOption{
		updateSourceInterval: synchronizeInterval,
		updateTargetInterval: synchronizeInterval,
	}
}

func WithSyncSourceInterval(interval time.Duration) Option {
	return func(option *synchronizerOption) {
		option.updateSourceInterval = interval
	}
}

func WithSyncTargetInterval(interval time.Duration) Option {
	return func(option *synchronizerOption) {
		option.updateTargetInterval = interval
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

func (fac *factory) GetSynchronizer(source, target SynchronizerObject) (DynamicSynchronizer, bool) {
	key := fmt.Sprintf("%s-%s", utils.FormActorKey(source.ClusterInformer), utils.FormActorKey(target.ClusterInformer))
	cluster, ok := fac.synchronizerMap.Get(key)
	if !ok {
		return nil, false
	}
	sync, ok := cluster.Get(formGVRKey(source.GVR, target.GVR))
	if !ok {
		return nil, false
	}
	return sync, true
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

	sync, ok := cluster.Get(formGVRKey(source.GVR, target.GVR))
	if !ok {
		sourceAPI := fac.GetResourceInterface(source.ClusterK8sAPI).Resource(source.GVR)
		targetAPI := fac.GetResourceInterface(target.ClusterK8sAPI).Resource(target.GVR)
		builder := newSynchronizerBuilder(sourceAPI, targetAPI, syncOpt.updateSourceInterval, syncOpt.updateTargetInterval)
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
		sync = builder.CreateSynchronizer()
		cluster.Set(formGVRKey(source.GVR, target.GVR), sync)
	}

	return sync, nil
}

func NewFactory(dyInformer informer.DynamicInformer, k8sBuilder kubernetes.Builder) SynchronizerFactory {
	return &factory{
		DynamicInformer: dyInformer,
		Builder:         k8sBuilder,
		synchronizerMap: clusterSelector{cmap.New[resourceSelector]()},
	}
}

func formGVRKey(sourceGVR, targetGVR schema.GroupVersionResource) string {
	return fmt.Sprintf("%s.%s.%s-%s.%s.%s", sourceGVR.Group, sourceGVR.Version, sourceGVR.Resource,
		targetGVR.Group, targetGVR.Version, targetGVR.Resource)
}
