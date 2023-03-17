package synchronize

import (
	"context"
	"fmt"
	"sync"
	"time"

	cmap "github.com/orcaman/concurrent-map"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/informer"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/kubernetes"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/synchronize/coresync"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type retryableSynchronizer struct {
	informer informer.DynamicInformer
	k8s      kubernetes.Builder
	once     sync.Once

	builder coresync.Builder[DynamicLabelObject, DynamicLabelObject]
	core    *synchronizerCore

	source SynchronizerObject
	target SynchronizerObject

	sSubscriber *informer.DynamicSubscribe
	tSubscriber *informer.DynamicSubscribe
}

func newRetryableSynchronizer(
	informer informer.DynamicInformer,
	k8s kubernetes.Builder,
	source SynchronizerObject,
	target SynchronizerObject,
	core *synchronizerCore,
) *retryableSynchronizer {
	return &retryableSynchronizer{
		builder:  coresync.NewBuilder[DynamicLabelObject, DynamicLabelObject](),
		informer: informer,
		k8s:      k8s,
		source:   source,
		target:   target,
		core:     core,
		once:     sync.Once{},
	}
}

// GetSynchronizeAssignment check whether target has been assigned.
func (s *retryableSynchronizer) GetSynchronizeAssignment(namespace, name string) bool {
	query := createQuery(s.target.ClusterK8sAPI.Address, namespace, name)
	_, ok := s.core.synchronizer.GetUpstreamFromDownstream(query)
	if !ok {
		return false
	}
	return true
}

// RemoveSynchronizeAssignment remove synchronizing target.
func (s *retryableSynchronizer) RemoveSynchronizeAssignment(namespace, name string) {
	target := createQuery(s.target.ClusterInformer.Address, namespace, name)
	s.core.synchronizer.DeleteDownstream(target)
}

func (s *retryableSynchronizer) AddSynchronizeAssignments(op *Operation, tasks ...DynamicAssignment) error {
	if len(tasks) == 0 {
		return fmt.Errorf("can not add an empty tasks")
	}

	if op == nil {
		op = &Operation{
			MappingNamespace: nil,
			MappingNames:     make(map[string]string),
		}
	}

	for _, t := range tasks {
		err := s.binding(op, t)
		if err != nil {
			return err
		}
	}

	if s.sSubscriber != nil || s.tSubscriber != nil {
		return nil
	}

	err := s.dynamicBuildCore(tasks)
	if err != nil {
		return err
	}
	return nil
}

func (s *retryableSynchronizer) dynamicBuildCore(tasks []DynamicAssignment) error {
	upstreamTriggers, downstreamTriggers := s.createTriggers(tasks)

	sInformer := newLabeledInformer[unstructured.Unstructured](Cluster{
		Address: s.source.ClusterInformer.Address,
	})
	sSubscriber := informer.NewDynamicSubscribe(s.source.ClusterInformer, s.source.GVR,
		informer.WithDynamicInformerHandler(sInformer.CreateDynamicEventHandler()))

	tInformer := newLabeledInformer[unstructured.Unstructured](Cluster{
		Address: s.target.ClusterInformer.Address,
	})
	tSubscriber := informer.NewDynamicSubscribe(s.target.ClusterInformer, s.target.GVR,
		informer.WithDynamicInformerHandler(tInformer.CreateDynamicEventHandler()))

	s.sSubscriber = &sSubscriber
	s.tSubscriber = &tSubscriber

	err := s.core.synchronizer.SetUpstreamStreamer(sInformer)
	if err != nil {
		return err
	}

	err = s.core.synchronizer.SetUpstreamOperator(upstreamTriggers...)
	if err != nil {
		return err
	}

	s.core.searcher.addSourceOperator(s.source.ClusterK8sAPI.Address,
		s.k8s.GetResourceInterface(s.source.ClusterK8sAPI).Resource(s.source.GVR))

	err = s.core.synchronizer.SetDownstreamStreamer(tInformer)
	if err != nil {
		return err
	}

	s.core.searcher.addTargetOperator(s.target.ClusterK8sAPI.Address,
		s.k8s.GetResourceInterface(s.target.ClusterK8sAPI).Resource(s.target.GVR))

	err = s.core.synchronizer.SetDownstreamOperator(downstreamTriggers...)
	if err != nil {
		return err
	}
	return nil
}

func (s *retryableSynchronizer) Run(ctx context.Context) {
	if s.tSubscriber == nil || s.sSubscriber == nil {
		logrus.Errorf("can not run retryable synchronizer due to subscriber is empty," +
			"add a task before run synchronizer.")
		return
	}
	s.run(ctx)
}

func (s *retryableSynchronizer) run(ctx context.Context) {
	s.once.Do(func() {
		done := make(chan struct{})
		go func() {
			s.core.synchronizer.Run(ctx)
			if _, ok := <-done; !ok {
				return
			}
			done <- struct{}{}
		}()

		_, err := s.informer.SetResourceHandler(*s.sSubscriber, *s.tSubscriber)
		if err != nil {
			logrus.Errorf("can not run retryable synchronizer due to %s", err)
			return
		}
		<-done
		logrus.Infof("retry synchronizer has been closed")
	})
}

func (s *retryableSynchronizer) createTriggers(tasks []DynamicAssignment) ([]DynamicUpstreamTrigger, []DynamicDownstreamTrigger) {
	upstreamTriggers := make([]DynamicUpstreamTrigger, len(tasks))
	downstreamTriggers := make([]DynamicDownstreamTrigger, len(tasks))
	for i, t := range tasks {
		upstreamTriggers[i] = newAssignmentUpstreamTrigger(
			&Cluster{Address: s.target.ClusterInformer.Address},
			t.SynchronizeTarget(),
			s.k8s.GetResourceInterface(s.target.ClusterK8sAPI).Resource(s.target.GVR),
		)
		downstreamTriggers[i] = newAssignmentDownstreamTrigger(
			&Cluster{Address: s.source.ClusterInformer.Address},
			t.SynchronizeSource(),
			s.k8s.GetResourceInterface(s.source.ClusterK8sAPI).Resource(s.source.GVR),
		)
	}
	return upstreamTriggers, downstreamTriggers
}

func (s *retryableSynchronizer) binding(op *Operation, t DynamicAssignment) error {
	sourceRes := &unstructured.Unstructured{}
	sourceRes.SetName(t.GetName())
	sourceRes.SetNamespace(t.GetNamespace())
	source := &DynamicLabelObject{
		Label:    map[string]string{ClusterLabel: s.source.ClusterInformer.Address},
		Resource: sourceRes,
	}

	targetRes := &unstructured.Unstructured{}
	targetRes.SetNamespace(t.GetNamespace())
	if op.MappingNamespace != nil {
		targetRes.SetNamespace(*op.MappingNamespace)
	}

	targetRes.SetName(t.GetName())
	if len(op.MappingNames) != 0 {
		if name, ok := op.MappingNames[t.GetName()]; ok {
			targetRes.SetName(name)
		}
	}

	target := &DynamicLabelObject{
		Label:    map[string]string{ClusterLabel: s.target.ClusterInformer.Address},
		Resource: targetRes,
	}

	err := s.core.synchronizer.CreateBind(source, target)
	if err != nil {
		return fmt.Errorf("retryable synchronizer binding error: %v", err)
	}
	return nil
}

type clusterSearcher struct {
	targetOperators cmap.ConcurrentMap[kubernetes.DynamicNamespaceableOperator]
	sourceOperators cmap.ConcurrentMap[kubernetes.DynamicNamespaceableOperator]
}

func newClusterSearcher() *clusterSearcher {
	return &clusterSearcher{
		targetOperators: cmap.New[kubernetes.DynamicNamespaceableOperator](),
		sourceOperators: cmap.New[kubernetes.DynamicNamespaceableOperator](),
	}
}

func (g *clusterSearcher) addTargetOperator(cluster string, operator kubernetes.DynamicNamespaceableOperator) {
	g.targetOperators.Set(cluster, operator)
}

func (g *clusterSearcher) addSourceOperator(cluster string, operator kubernetes.DynamicNamespaceableOperator) {
	g.sourceOperators.Set(cluster, operator)
}

func (g *clusterSearcher) GetTarget(target *DynamicLabelObject) (*DynamicLabelObject, bool) {
	if target == nil {
		return nil, false
	}

	operator, err := g.getOperator(target, g.targetOperators)
	if err != nil {
		logrus.Debugf("can not get operator due to %v", err)
		return nil, false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := operator.Namespace(target.Resource.GetNamespace()).Get(ctx, target.Resource.GetName(), metav1.GetOptions{})
	if err != nil {
		return nil, false
	}
	result := &DynamicLabelObject{
		Label:    target.Label,
		Resource: res,
	}
	return result, true
}

func (g *clusterSearcher) ListTarget() []*DynamicLabelObject {
	result := []*DynamicLabelObject{}

	for clusterName, operator := range g.targetOperators.Items() {
		resList, err := operator.ListSlice(context.Background(), metav1.ListOptions{})
		if err != nil {
			logrus.Errorf("can not list resource due to %v", err)
			return nil
		}

		for _, res := range resList {
			result = append(result, &DynamicLabelObject{
				Label:    map[string]string{ClusterLabel: clusterName},
				Resource: res.DeepCopy(),
			})
		}
	}
	return result
}

func (g *clusterSearcher) GetSource(source *DynamicLabelObject) (*DynamicLabelObject, bool) {
	if source == nil {
		return nil, false
	}

	operator, err := g.getOperator(source, g.sourceOperators)
	if err != nil {
		logrus.Debugf("can not get operator due to %v", err)
		return nil, false
	}

	res, err := operator.Namespace(source.Resource.GetNamespace()).Get(context.Background(),
		source.Resource.GetName(), metav1.GetOptions{})
	if err != nil {
		return nil, false
	}
	result := &DynamicLabelObject{
		Label:    source.Label,
		Resource: res,
	}
	return result, true
}

func (g *clusterSearcher) ListSource() []*DynamicLabelObject {
	result := []*DynamicLabelObject{}
	for clusterName, operator := range g.sourceOperators.Items() {
		resList, err := operator.ListSlice(context.Background(), metav1.ListOptions{})
		if err != nil {
			logrus.Errorf("can not list source resource due to %v", err)
			return nil
		}

		for _, res := range resList {
			result = append(result, &DynamicLabelObject{
				Label:    map[string]string{ClusterLabel: clusterName},
				Resource: res.DeepCopy(),
			})
		}
	}
	return result
}

func (g *clusterSearcher) getOperator(obj *DynamicLabelObject,
	operatorMap cmap.ConcurrentMap[kubernetes.DynamicNamespaceableOperator]) (kubernetes.DynamicNamespaceableOperator, error) {
	clusterName, ok := obj.Label[ClusterLabel]
	if !ok {
		return nil, fmt.Errorf("cluster label can not be found")
	}

	operator, ok := operatorMap.Get(clusterName)
	if !ok {
		logrus.Debugf("can not get operator on cluster named %s", clusterName)
		return nil, fmt.Errorf("can not get operator on cluster named %s", clusterName)
	}
	return operator, nil
}

func createQuery(cluster string, namespace, name string) *DynamicLabelObject {
	queryObj := unstructured.Unstructured{}
	queryObj.SetName(name)
	queryObj.SetNamespace(namespace)
	query := DynamicLabelObject{
		Label:    map[string]string{ClusterLabel: cluster},
		Resource: &queryObj,
	}
	return &query
}
