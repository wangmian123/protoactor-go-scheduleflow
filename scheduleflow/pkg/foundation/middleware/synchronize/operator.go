package synchronize

import (
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/kubernetes"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/synchronize/coresync"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type dynamicDownstreamTrigger struct {
	upstreamCluster *Cluster
	task            DynamicResourceMutator
	upstreamAPI     kubernetes.DynamicNamespaceableOperator
}

func newAssignmentDownstreamTrigger(cluster *Cluster, syncSource DynamicResourceMutator,
	api kubernetes.DynamicNamespaceableOperator) DynamicDownstreamTrigger {
	return &dynamicDownstreamTrigger{
		upstreamCluster: cluster,
		task:            syncSource,
		upstreamAPI:     api,
	}
}

func (ope *dynamicDownstreamTrigger) DownstreamInflow(source *DynamicLabelObject, inflow *DynamicLabelObject,
	rest, missing []*DynamicLabelObject) error {
	if value, ok := source.Label[ClusterLabel]; !ok {
		return nil
	} else if value != ope.upstreamCluster.LabelCluster() {
		return nil
	}

	result, err := ope.task.SynchronizeAddedResource(inflow.Resource, source.Resource)
	if err != nil {
		return err
	}
	_, err = mutateResource(ope.upstreamAPI, result)
	if err != nil {
		return err
	}
	return nil
}

func (ope *dynamicDownstreamTrigger) DownstreamUpdate(source *DynamicLabelObject, update *DynamicResourceUpdater,
	rest, missing []*DynamicLabelObject) error {
	if value, ok := source.Label[ClusterLabel]; !ok {
		return nil
	} else if value != ope.upstreamCluster.LabelCluster() {
		return nil
	}

	result, err := ope.task.SynchronizeUpdatedResource(getUpdatedOldResource(update), update.Newest.Resource, source.Resource)
	if err != nil {
		return err
	}
	_, err = mutateResource(ope.upstreamAPI, result)
	if err != nil {
		return err
	}
	return nil
}

func (ope *dynamicDownstreamTrigger) DownstreamOutflow(source *DynamicLabelObject, outflow *DynamicLabelObject,
	rest, missing []*DynamicLabelObject) error {
	if value, ok := source.Label[ClusterLabel]; !ok {
		return nil
	} else if value != ope.upstreamCluster.LabelCluster() {
		return nil
	}

	result, err := ope.task.SynchronizeDeletedResource(outflow.Resource, source.Resource)
	if err != nil {
		return err
	}
	_, err = mutateResource(ope.upstreamAPI, result)
	if err != nil {
		return err
	}
	return nil
}

type dynamicUpstreamTrigger struct {
	downstreamCluster *Cluster
	task              DynamicResourceMutator
	downstreamAPI     kubernetes.DynamicNamespaceableOperator
}

func newAssignmentUpstreamTrigger(cluster *Cluster, syncSource DynamicResourceMutator,
	api kubernetes.DynamicNamespaceableOperator) DynamicUpstreamTrigger {
	return &dynamicUpstreamTrigger{
		downstreamCluster: cluster,
		task:              syncSource,
		downstreamAPI:     api,
	}
}

func (ope *dynamicUpstreamTrigger) UpstreamInflow(source *DynamicLabelObject, rest, missing []*DynamicLabelObject) error {
	mutateNormalFunc := func(r *DynamicLabelObject) (*DynamicMutatedResult, error) {
		return ope.task.SynchronizeAddedResource(source.Resource, r.Resource)
	}

	err := ope.mutateTarget(rest, mutateNormalFunc)
	if err != nil {
		return err
	}

	mutateMissingFunc := func(r *DynamicLabelObject) (*DynamicMutatedResult, error) {
		return ope.task.SynchronizeAddedResource(source.Resource, nil)
	}

	if len(missing) == 0 {
		return nil
	}

	err = ope.mutateTarget(missing, mutateMissingFunc)
	if err != nil {
		return err
	}
	return nil
}

func (ope *dynamicUpstreamTrigger) UpstreamUpdate(source *coresync.ResourceUpdater[DynamicLabelObject], rest, missing []*DynamicLabelObject) error {
	mutateNormalFunc := func(r *DynamicLabelObject) (*DynamicMutatedResult, error) {
		return ope.task.SynchronizeUpdatedResource(getUpdatedOldResource(source), source.Newest.Resource, r.Resource)
	}

	err := ope.mutateTarget(rest, mutateNormalFunc)
	if err != nil {
		return err
	}

	mutateMissingFunc := func(r *DynamicLabelObject) (*DynamicMutatedResult, error) {
		return ope.task.SynchronizeUpdatedResource(getUpdatedOldResource(source), source.Newest.Resource, nil)
	}

	if len(missing) == 0 {
		return nil
	}

	err = ope.mutateTarget(missing, mutateMissingFunc)
	if err != nil {
		return err
	}
	return nil
}

func (ope *dynamicUpstreamTrigger) UpstreamOutflow(source *DynamicLabelObject, rest, missing []*DynamicLabelObject) error {
	mutateNormalFunc := func(r *DynamicLabelObject) (*DynamicMutatedResult, error) {
		return ope.task.SynchronizeDeletedResource(source.Resource, r.Resource)
	}

	err := ope.mutateTarget(rest, mutateNormalFunc)
	if err != nil {
		return err
	}

	mutateMissingFunc := func(r *DynamicLabelObject) (*DynamicMutatedResult, error) {
		return ope.task.SynchronizeAddedResource(source.Resource, nil)
	}

	if len(missing) == 0 {
		return nil
	}

	err = ope.mutateTarget(missing, mutateMissingFunc)
	if err != nil {
		return err
	}
	return nil
}

func (ope *dynamicUpstreamTrigger) mutateTarget(targets []*DynamicLabelObject,
	runFunc func(*DynamicLabelObject) (*DynamicMutatedResult, error)) error {
	for _, r := range targets {
		if value, ok := r.Label[ClusterLabel]; !ok {
			continue
		} else if value != ope.downstreamCluster.LabelCluster() {
			continue
		}

		result, err := runFunc(r)
		if err != nil {
			return err
		}

		_, err = mutateResource(ope.downstreamAPI, result)
		if err != nil {
			return err
		}
	}
	return nil
}

func getUpdatedOldResource(updated *coresync.ResourceUpdater[DynamicLabelObject]) *unstructured.Unstructured {
	if updated == nil {
		return nil
	}
	if updated.ProbableOld == nil {
		return nil
	}

	return updated.ProbableOld.Resource
}
