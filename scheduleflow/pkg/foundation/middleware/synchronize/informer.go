package synchronize

import (
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/informer"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/synchronize/coresync"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const channelCache = 1000

type labeledInformer struct {
	cluster Cluster

	whenAdd    coresync.CreationStreamChannel[LabeledObject[unstructured.Unstructured]]
	whenDelete coresync.CreationStreamChannel[LabeledObject[unstructured.Unstructured]]
	whenUpdate coresync.UpdatingStreamChannel[LabeledObject[unstructured.Unstructured]]
}

func newLabeledInformer[T any](cluster Cluster) *labeledInformer {
	return &labeledInformer{
		cluster:    cluster,
		whenAdd:    make(coresync.CreationStreamChannel[LabeledObject[unstructured.Unstructured]], channelCache),
		whenDelete: make(coresync.CreationStreamChannel[LabeledObject[unstructured.Unstructured]], channelCache),
		whenUpdate: make(coresync.UpdatingStreamChannel[LabeledObject[unstructured.Unstructured]], channelCache),
	}
}

func (m *labeledInformer) whenResourceAdd(res unstructured.Unstructured) {
	label := make(map[string]string)
	label[ClusterLabel] = m.cluster.LabelCluster()
	m.whenAdd <- &LabeledObject[unstructured.Unstructured]{Label: label, Resource: res.DeepCopy()}
}

func (m *labeledInformer) whenResourceDelete(res unstructured.Unstructured) {
	label := make(map[string]string)
	label[ClusterLabel] = m.cluster.LabelCluster()
	m.whenDelete <- &LabeledObject[unstructured.Unstructured]{Label: label, Resource: res.DeepCopy()}
}

func (m *labeledInformer) whenResourceUpdate(oldRes, newRes unstructured.Unstructured) {
	labelOld, labelNew := make(map[string]string), make(map[string]string)
	labelOld[ClusterLabel] = m.cluster.LabelCluster()
	labelNew[ClusterLabel] = m.cluster.LabelCluster()
	updated := &coresync.ResourceUpdater[LabeledObject[unstructured.Unstructured]]{
		ProbableOld: &LabeledObject[unstructured.Unstructured]{Label: labelOld, Resource: oldRes.DeepCopy()},
		Newest:      &LabeledObject[unstructured.Unstructured]{Label: labelNew, Resource: newRes.DeepCopy()},
	}
	m.whenUpdate <- updated
}

func (m *labeledInformer) CreateDynamicEventHandler() informer.DynamicEventHandler {
	return &informer.HandlerFunc{
		Creating: m.whenResourceAdd,
		Updating: m.whenResourceUpdate,
		Deleting: m.whenResourceDelete,
	}
}

func (m *labeledInformer) CreateLabeledInformer() coresync.Informer[LabeledObject[unstructured.Unstructured]] {
	return m
}

func (m *labeledInformer) InflowChannel() coresync.CreationStreamChannel[LabeledObject[unstructured.Unstructured]] {
	return m.whenAdd
}

func (m *labeledInformer) UpdateChannel() coresync.UpdatingStreamChannel[LabeledObject[unstructured.Unstructured]] {
	return m.whenUpdate
}

func (m *labeledInformer) OutflowChannel() coresync.CreationStreamChannel[LabeledObject[unstructured.Unstructured]] {
	return m.whenDelete
}
