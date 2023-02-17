package synchronize

import (
	"context"
	"fmt"
	"time"

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

	builder coresync.Builder[DynamicLabelObject, DynamicLabelObject]
	core    coresync.Synchronizer[DynamicLabelObject, DynamicLabelObject]

	source SynchronizerObject
	target SynchronizerObject
	tasks  []DynamicAssignment
}

func newRetryableSynchronizer(
	informer informer.DynamicInformer,
	k8s kubernetes.Builder,
	source SynchronizerObject,
	target SynchronizerObject,
) DynamicSynchronizer {
	return &retryableSynchronizer{
		builder:  coresync.NewBuilder[DynamicLabelObject, DynamicLabelObject](),
		informer: informer,
		k8s:      k8s,
		source:   source,
		target:   target,
	}
}

func (s *retryableSynchronizer) GetSynchronizeAssignment(namespace, name string) (DynamicAssignment, bool) {
	query := createQuery(s.source.ClusterK8sAPI.Address, namespace, name)
	if s.core == nil {
		return nil, false
	}
	_, ok := s.core.GetSyncUpstream(query)
	if !ok {
		return nil, false
	}
	return nil, true
}

func (s *retryableSynchronizer) RemoveSynchronizeAssignment(namespace, name string) {
	deleted := createQuery(s.source.ClusterInformer.Address, namespace, name)
	if s.core == nil {
		return
	}
	s.core.DeleteBind(deleted, deleted)
}

func (s *retryableSynchronizer) AddSynchronizeAssignments(op *Operation, tasks ...DynamicAssignment) error {
	if len(tasks) == 0 {
		return fmt.Errorf("can not add an empty tasks")
	}
	s.tasks = append(s.tasks, tasks...)
	return nil
}

func (s *retryableSynchronizer) Run(ctx context.Context) {
	recordFunc := func(object *DynamicLabelObject) string {
		return formClusterResourceKey(object.Label[ClusterLabel], object.Resource.GetNamespace(),
			object.Resource.GetName())
	}

	sInformer := newLabeledInformer[unstructured.Unstructured](Cluster{
		Address: s.source.ClusterInformer.Address,
	})
	sDescriber := informer.NewDynamicSubscribe(s.source.ClusterInformer, s.source.GVR,
		informer.WithDynamicInformerHandler(sInformer.CreateDynamicEventHandler()))

	tInformer := newLabeledInformer[unstructured.Unstructured](Cluster{
		Address: s.target.ClusterInformer.Address,
	})
	tDescriber := informer.NewDynamicSubscribe(s.target.ClusterInformer, s.target.GVR,
		informer.WithDynamicInformerHandler(tInformer.CreateDynamicEventHandler()))

	upstreamTriggers, downstreamTriggers := s.createTriggers()

	core, err := s.builder.SetRecorder(recordFunc, recordFunc).SetSearcher(s.createSearcher()).
		SetBilateralStreamer([]coresync.Informer[DynamicLabelObject]{sInformer}, []coresync.Informer[DynamicLabelObject]{tInformer}).
		SetBilateralOperator(upstreamTriggers, downstreamTriggers).CreateSynchronizer()
	if err != nil {
		logrus.Errorf("create retryable core synchronizer with error: %v", err)
		return
	}
	s.core = core

	for _, t := range s.tasks {
		err = s.binding(t)
		if err != nil {
			logrus.Error(err)
		}
	}

	done := make(chan struct{})
	go func() {
		s.core.Run(ctx)
		done <- struct{}{}
	}()
	_, err = s.informer.SetResourceHandler(sDescriber, tDescriber)
	if err != nil {
		logrus.Errorf("start informer with error: %v", err)
		return
	}
	<-done
	logrus.Infof("retry synchronizer has been closed")
}

func (s *retryableSynchronizer) createTriggers() ([]DynamicUpstreamTrigger, []DynamicDownstreamTrigger) {
	upstreamTriggers := make([]DynamicUpstreamTrigger, len(s.tasks))
	downstreamTriggers := make([]DynamicDownstreamTrigger, len(s.tasks))
	for i, t := range s.tasks {
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

func (s *retryableSynchronizer) binding(t DynamicAssignment) error {
	sourceRes := &unstructured.Unstructured{}
	sourceRes.SetName(t.GetName())
	sourceRes.SetNamespace(t.GetNamespace())
	source := &DynamicLabelObject{
		Label:    map[string]string{ClusterLabel: s.source.ClusterInformer.Address},
		Resource: sourceRes,
	}

	targetRes := &unstructured.Unstructured{}
	targetRes.SetName(t.GetName())
	targetRes.SetNamespace(t.GetNamespace())
	target := &DynamicLabelObject{
		Label:    map[string]string{ClusterLabel: s.target.ClusterInformer.Address},
		Resource: targetRes,
	}

	err := s.core.CreateBind(source, target)
	if err != nil {
		return fmt.Errorf("retryable synchronizer binding error: %v", err)
	}
	return nil
}

func (s *retryableSynchronizer) createSearcher() coresync.ObjectSearcher[DynamicLabelObject, DynamicLabelObject] {
	sOperator := s.k8s.GetResourceInterface(kubernetes.GetAPI(s.source.ClusterK8sAPI.Address)).Resource(s.source.GVR)
	tOperator := s.k8s.GetResourceInterface(kubernetes.GetAPI(s.target.ClusterK8sAPI.Address)).Resource(s.target.GVR)
	searcher := newClusterSearcher(s.source.ClusterK8sAPI.Address, s.target.ClusterK8sAPI.Address, sOperator, tOperator)
	return searcher
}

type clusterSearcher struct {
	sCluster  string
	tCluster  string
	sOperator kubernetes.NamespaceableResourceInterface[unstructured.Unstructured]
	tOperator kubernetes.NamespaceableResourceInterface[unstructured.Unstructured]
}

func newClusterSearcher(
	sCluster string,
	tCluster string,
	sOperator kubernetes.NamespaceableResourceInterface[unstructured.Unstructured],
	tOperator kubernetes.NamespaceableResourceInterface[unstructured.Unstructured],
) coresync.ObjectSearcher[DynamicLabelObject, DynamicLabelObject] {

	return &clusterSearcher{
		sCluster:  sCluster,
		tCluster:  tCluster,
		sOperator: sOperator,
		tOperator: tOperator,
	}
}

func (g *clusterSearcher) GetTarget(target *DynamicLabelObject) (*DynamicLabelObject, bool) {
	if target == nil {
		return nil, false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := g.tOperator.Namespace(target.Resource.GetNamespace()).Get(ctx, target.Resource.GetName(), metav1.GetOptions{})
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
	resList, err := g.tOperator.ListSlice(context.Background(), metav1.ListOptions{})
	if err != nil {
		logrus.Errorf("can not list resource due to %v", err)
		return nil
	}

	for _, res := range resList {
		result = append(result, &DynamicLabelObject{
			Label:    map[string]string{ClusterLabel: g.tCluster},
			Resource: res.DeepCopy(),
		})
	}
	return result
}

func (g *clusterSearcher) GetSource(source *DynamicLabelObject) (*DynamicLabelObject, bool) {
	if source == nil {
		return nil, false
	}

	res, err := g.sOperator.Namespace(source.Resource.GetNamespace()).Get(context.Background(),
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
	resList, err := g.sOperator.ListSlice(context.Background(), metav1.ListOptions{})
	if err != nil {
		logrus.Errorf("can not list source resource due to %v", err)
		return nil
	}

	for _, res := range resList {
		result = append(result, &DynamicLabelObject{
			Label:    map[string]string{ClusterLabel: g.sCluster},
			Resource: res.DeepCopy(),
		})
	}
	return result
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
