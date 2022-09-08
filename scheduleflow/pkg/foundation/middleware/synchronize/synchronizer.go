package synchronize

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/informer"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/kubernetes"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
	"github.com/wI2L/jsondiff"
	"google.golang.org/protobuf/types/known/durationpb"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

type Mutate func(source, target *unstructured.Unstructured) (*unstructured.Unstructured, error)

const maxUpdateAttempts = 5

type nameSelector struct {
	cmap.ConcurrentMap[*operationalAssignment]
}

type namespaceSelector struct {
	cmap.ConcurrentMap[nameSelector]
}

type mappingAssignmentInfo struct {
	mappingNamespace string
	mappingName      string
}

type operationalAssignment struct {
	GenericAssignment[unstructured.Unstructured, unstructured.Unstructured]
	mappingAssignmentInfo
}

func newOperationalAssignment(mapping mappingAssignmentInfo, task DynamicAssignment) *operationalAssignment {
	if task == nil {
		return nil
	}

	return &operationalAssignment{
		GenericAssignment:     task,
		mappingAssignmentInfo: mapping,
	}
}

type synchronizer struct {
	kubernetesAPI kubernetes.DynamicNamespaceableOperator
	resourceStore informer.UnstructuredStore
	assignmentMap namespaceSelector
	miniInterval  time.Duration
}

func newSynchronizer(api kubernetes.DynamicNamespaceableOperator, interval time.Duration) *synchronizer {
	return &synchronizer{
		kubernetesAPI: api,
		resourceStore: nil,
		assignmentMap: namespaceSelector{cmap.New[nameSelector]()},
		miniInterval:  interval,
	}
}

type synchronizerPair struct {
	target synchronizer
	source synchronizer
}

func newSynchronizerBuilder(sourceAPI, targetAPI kubernetes.DynamicNamespaceableOperator, sourceGap, targetGap time.Duration) DynamicSynchronizerBuilder {
	return &synchronizerPair{
		source: synchronizer{
			kubernetesAPI: sourceAPI,
			assignmentMap: namespaceSelector{cmap.New[nameSelector]()},
			miniInterval:  sourceGap / maxUpdateAttempts,
		},
		target: synchronizer{
			kubernetesAPI: targetAPI,
			assignmentMap: namespaceSelector{cmap.New[nameSelector]()},
			miniInterval:  targetGap / maxUpdateAttempts,
		},
	}
}

func (sync *synchronizerPair) GetSynchronizeAssignment(namespace, name string) (DynamicAssignment, bool) {
	sourceMap, ok := sync.source.assignmentMap.Get(namespace)
	if !ok {
		return nil, false
	}
	task, ok := sourceMap.Get(name)
	if !ok {
		return nil, false
	}
	return task, true
}

func (sync *synchronizerPair) RemoveSynchronizeAssignment(namespace, name string) {
	sourceMap, ok := sync.source.assignmentMap.Get(namespace)
	if !ok {
		return
	}
	operableTask, ok := sourceMap.Get(name)
	if !ok {
		return
	}
	sourceMap.Remove(name)
	targetMap, ok := sync.target.assignmentMap.Get(operableTask.mappingNamespace)
	if !ok {
		logrus.Warningf("can not get mapping namespace: %s", operableTask.mappingNamespace)
		return
	}
	_, ok = targetMap.Get(operableTask.mappingName)
	if ok {
		targetMap.Remove(operableTask.mappingName)
		return
	}
	logrus.Warningf("can not get mapping task: %s/%s", operableTask.mappingNamespace, operableTask.mappingName)
}

func (sync *synchronizerPair) AddSynchronizeAssignments(op *Operation, tasks ...DynamicAssignment) error {
	sourceList, err := sync.source.kubernetesAPI.ListSlice(context.Background(), v1.ListOptions{})
	if err != nil {
		return err
	}
	if op == nil {
		op = &Operation{
			MappingNamespace: "",
			MappingNames:     make(map[string]string),
		}
	}

	sourceMap := utils.ResourceListToMap[*unstructured.Unstructured](sourceList)
	for _, t := range tasks {
		if t == nil {
			logrus.Warningf("can not add assignment due to a nil task")
			continue
		}
		operationalTask := sync.addSynchronizeAssignment(op, t)
		res, ok := getResource(sourceMap, operationalTask.GetNamespace(), operationalTask.GetName())
		if !ok {
			continue
		}
		sync.onSourceAdd(*res)
	}
	return nil
}

// addSynchronizeAssignment mapping task to synchronizerPair source map and target map.
func (sync *synchronizerPair) addSynchronizeAssignment(op *Operation, task DynamicAssignment) *operationalAssignment {
	mapping := sync.generateMappingAssignmentInfo(op, task)
	operationalTask := newOperationalAssignment(mapping, task)
	namespace := task.GetNamespace()
	sourceMap, ok := sync.source.assignmentMap.Get(namespace)
	if !ok {
		sourceMap = nameSelector{ConcurrentMap: cmap.New[*operationalAssignment]()}
		sync.source.assignmentMap.Set(namespace, sourceMap)
	}

	targetMap, ok := sync.target.assignmentMap.Get(mapping.mappingNamespace)
	if !ok {
		targetMap = nameSelector{ConcurrentMap: cmap.New[*operationalAssignment]()}
		sync.target.assignmentMap.Set(mapping.mappingNamespace, targetMap)
	}

	_, ok = sourceMap.Get(task.GetName())
	if ok {
		logrus.Warningf("overwright a synchronizing assignment in source map")
	}

	_, ok = targetMap.Get(mapping.mappingName)
	if ok {
		logrus.Warningf("overwright a synchronizing assignment in target map")
	}

	sourceMap.Set(task.GetName(), operationalTask)
	targetMap.Set(mapping.mappingName, operationalTask)
	return operationalTask
}

func (sync *synchronizerPair) generateMappingAssignmentInfo(op *Operation, task DynamicAssignment) mappingAssignmentInfo {
	namespace := task.GetNamespace()

	targetNamespace := namespace
	if op.MappingNamespace != AllNamespace {
		targetNamespace = op.MappingNamespace
	}

	targetName := task.GetName()
	if len(op.MappingNames) != 0 {
		mappingTargetName, ok := op.MappingNames[task.GetName()]
		if ok {
			targetName = mappingTargetName
		}
	}
	return mappingAssignmentInfo{
		mappingNamespace: targetNamespace,
		mappingName:      targetName,
	}
}

func (sync *synchronizerPair) onSourceAdd(resource unstructured.Unstructured) {
	as, ok := sync.findSourceAssignment(resource.GetNamespace(), resource.GetName())
	if !ok {
		return
	}

	target, err := sync.target.kubernetesAPI.Namespace(as.mappingNamespace).
		BlockGet(context.Background(), as.mappingName, kubeproxy.BlockGetOptions{BlockTimeout: durationpb.New(sync.target.miniInterval)})

	var mutatedTarget *DynamicMutatedResult
	if err != nil {
		mutatedTarget, err = as.SynchronizeTarget().SynchronizeAddedResource(resource.DeepCopy(), nil)
	} else {
		defer sync.source.kubernetesAPI.Namespace(as.GetNamespace()).UnlockResource(context.Background(), as.GetName())
		mutatedTarget, err = as.SynchronizeTarget().SynchronizeAddedResource(resource.DeepCopy(), target.DeepCopy())
	}

	if err != nil {
		logrus.Errorf("mutate synchronizing resource error: %v", err)
		return
	}

	if mutatedTarget == nil {
		return
	}

	if mutatedTarget.OriginalResource == nil {
		mutatedTarget.OriginalResource = target
	}

	_, err = sync.mutateResource(sync.target.kubernetesAPI, mutatedTarget)
	if err != nil {
		logrus.Debugf("synchronizing target resource with error: %v", err)
		return
	}
}

func (sync *synchronizerPair) onTargetAdd(resource unstructured.Unstructured) {
	as, ok := sync.findTargetAssignment(resource.GetNamespace(), resource.GetName())
	if !ok {
		return
	}

	source, err := sync.source.kubernetesAPI.Namespace(as.GetNamespace()).
		BlockGet(context.Background(), as.GetName(), kubeproxy.BlockGetOptions{BlockTimeout: durationpb.New(sync.source.miniInterval)})

	var mutatedSource *DynamicMutatedResult
	if err != nil {
		mutatedSource, err = as.SynchronizeTarget().SynchronizeAddedResource(resource.DeepCopy(), nil)
	} else {
		defer sync.source.kubernetesAPI.Namespace(as.GetNamespace()).UnlockResource(context.Background(), as.GetName())
		mutatedSource, err = as.SynchronizeTarget().SynchronizeAddedResource(resource.DeepCopy(), source.DeepCopy())
	}

	if err != nil {
		logrus.Errorf("mutate synchronizing resource error: %v", err)
		return
	}
	if mutatedSource == nil {
		return
	}
	if mutatedSource.OriginalResource == nil {
		mutatedSource.OriginalResource = source
	}

	_, err = sync.mutateResource(sync.source.kubernetesAPI, mutatedSource)
	if err != nil {
		logrus.Debugf("synchronizing source resource with error: %v", err)
		return
	}
}

func (sync *synchronizerPair) onSourceUpdate(oldRes, newRes unstructured.Unstructured) {
	as, ok := sync.findSourceAssignment(newRes.GetNamespace(), newRes.GetName())
	if !ok {
		return
	}

	target, err := sync.target.kubernetesAPI.Namespace(as.mappingNamespace).
		BlockGet(context.Background(), as.mappingName, kubeproxy.BlockGetOptions{BlockTimeout: durationpb.New(sync.target.miniInterval)})

	var mutatedTarget *DynamicMutatedResult
	if err != nil {
		mutatedTarget, err = as.SynchronizeTarget().SynchronizeUpdatedResource(oldRes.DeepCopy(), newRes.DeepCopy(), nil)
	} else {
		defer sync.source.kubernetesAPI.Namespace(as.GetNamespace()).UnlockResource(context.Background(), as.GetName())
		mutatedTarget, err = as.SynchronizeTarget().SynchronizeUpdatedResource(oldRes.DeepCopy(), newRes.DeepCopy(), target.DeepCopy())
	}
	if err != nil {
		logrus.Errorf("mutate synchronizing resource error: %v", err)
		return
	}

	if mutatedTarget == nil {
		return
	}

	_, err = sync.mutateResource(sync.target.kubernetesAPI, mutatedTarget)
	if err != nil {
		logrus.Debugf("synchronizing target resource with error: %v", err)
		return
	}
}

func (sync *synchronizerPair) onTargetUpdate(oldRes, newRes unstructured.Unstructured) {
	as, ok := sync.findTargetAssignment(newRes.GetNamespace(), newRes.GetName())
	if !ok {
		return
	}

	source, err := sync.source.kubernetesAPI.Namespace(as.GetNamespace()).
		BlockGet(context.Background(), as.GetName(), kubeproxy.BlockGetOptions{BlockTimeout: durationpb.New(sync.source.miniInterval)})

	var mutatedSource *DynamicMutatedResult
	if err != nil {
		mutatedSource, err = as.SynchronizeSource().SynchronizeUpdatedResource(oldRes.DeepCopy(), newRes.DeepCopy(), nil)
	} else {
		defer sync.source.kubernetesAPI.Namespace(as.GetNamespace()).UnlockResource(context.Background(), as.GetName())
		mutatedSource, err = as.SynchronizeSource().SynchronizeUpdatedResource(oldRes.DeepCopy(), newRes.DeepCopy(), source.DeepCopy())
	}

	if err != nil {
		logrus.Errorf("mutate synchronizing resource error: %v", err)
		return
	}

	if mutatedSource == nil {
		return
	}

	_, err = sync.mutateResource(sync.source.kubernetesAPI, mutatedSource)
	if err != nil {
		logrus.Debugf("synchronizing source resource with error: %v", err)
		return
	}
}

func (sync *synchronizerPair) onSourceDelete(resource unstructured.Unstructured) {
	as, ok := sync.findSourceAssignment(resource.GetNamespace(), resource.GetName())
	if !ok {
		return
	}
	sync.deleteAssignment(as)

	target, err := sync.target.kubernetesAPI.Namespace(as.mappingNamespace).
		BlockGet(context.Background(), as.mappingName, kubeproxy.BlockGetOptions{BlockTimeout: durationpb.New(sync.target.miniInterval)})

	var mutatedTarget *DynamicMutatedResult
	if err != nil {
		mutatedTarget, err = as.SynchronizeTarget().SynchronizeDeletedResource(resource.DeepCopy(), nil)
	} else {
		defer sync.source.kubernetesAPI.Namespace(as.GetNamespace()).UnlockResource(context.Background(), as.GetName())
		mutatedTarget, err = as.SynchronizeTarget().SynchronizeDeletedResource(resource.DeepCopy(), target.DeepCopy())
	}

	if err != nil {
		logrus.Errorf("permite deleting source resource fail due to %v", err)
		return
	}

	if mutatedTarget.OriginalResource == nil {
		mutatedTarget.OriginalResource = target
	}

	if mutatedTarget != nil {
		_, err = sync.mutateResource(sync.target.kubernetesAPI, mutatedTarget)
		if err != nil {
			logrus.Debugf("synchronizing target resource fail onTargetDelete due to %v", err)
		}
	}
}

func (sync *synchronizerPair) onTargetDelete(resource unstructured.Unstructured) {
	as, ok := sync.findTargetAssignment(resource.GetNamespace(), resource.GetName())
	if !ok {
		return
	}

	source, err := sync.source.kubernetesAPI.Namespace(as.GetNamespace()).
		BlockGet(context.Background(), as.GetName(), kubeproxy.BlockGetOptions{BlockTimeout: durationpb.New(sync.source.miniInterval)})
	if err != nil {
		sync.deleteAssignment(as)
		return
	}

	defer sync.source.kubernetesAPI.Namespace(as.GetNamespace()).UnlockResource(context.Background(), as.GetName())

	var mutatedSource *DynamicMutatedResult
	mutatedSource, err = as.SynchronizeTarget().SynchronizeDeletedResource(resource.DeepCopy(), source.DeepCopy())
	if err != nil {
		logrus.Errorf("permite deleting source resource fail due to %v", err)
		return
	}

	if mutatedSource.OriginalResource == nil {
		mutatedSource.OriginalResource = source
	}

	if mutatedSource == nil {
		return
	}

	updatedSource, err := sync.mutateResource(sync.source.kubernetesAPI, mutatedSource)
	if err != nil {
		logrus.Debugf("synchronizing source resource fail onTargetDelete due to %v", err)
		return
	}

	if updatedSource == nil {
		sync.deleteAssignment(as)
	}
}

func (sync *synchronizerPair) mutateResource(api kubernetes.DynamicNamespaceableOperator, res *DynamicMutatedResult) (*unstructured.Unstructured, error) {
	if api == nil {
		return nil, fmt.Errorf("kubernetes client API is nil")
	}
	if res == nil {
		return nil, nil
	}
	switch res.Action {
	case Create:
		if res.MutatedResource == nil {
			return nil, fmt.Errorf("create a empty resource")
		}
		return api.Namespace(res.MutatedResource.GetNamespace()).Create(context.Background(), res.MutatedResource, v1.CreateOptions{})
	case Update:
		if res.MutatedResource == nil {
			return nil, fmt.Errorf("update a empty resource")
		}
		return api.Namespace(res.MutatedResource.GetNamespace()).Update(context.Background(), res.MutatedResource, v1.UpdateOptions{})
	case UpdateStatus:
		if res.MutatedResource == nil {
			return nil, fmt.Errorf("update a empty resource")
		}
		return api.Namespace(res.MutatedResource.GetNamespace()).UpdateStatus(context.Background(), res.MutatedResource, v1.UpdateOptions{})
	case Synchronize:
		return sync.synchronize(api, res)
	case Patch:
		return sync.patch(api, res)
	case PatchStatus:
		return sync.patchStatus(api, res)
	case Delete:
		if res.MutatedResource == nil {
			return nil, fmt.Errorf("delete a empty resource")
		}
		err := api.Namespace(res.MutatedResource.GetNamespace()).Delete(context.Background(), res.MutatedResource.GetName(), v1.DeleteOptions{})
		if err != nil {
			return nil, err
		}
		return nil, nil
	default:
		return nil, nil
	}
}

func (sync *synchronizerPair) patch(api kubernetes.DynamicNamespaceableOperator, res *DynamicMutatedResult) (*unstructured.Unstructured, error) {
	if res.MutatedResource == nil || res.OriginalResource == nil {
		return nil, fmt.Errorf("patch a empty resource")
	}

	patch, err := jsondiff.Compare(res.OriginalResource, res.MutatedResource)
	if err != nil {
		return nil, err
	}

	buffer, err := json.Marshal(patch)
	if err != nil {
		return nil, err
	}

	patched, err := api.Namespace(res.MutatedResource.GetNamespace()).Patch(context.Background(), res.MutatedResource.GetName(),
		types.JSONPatchType, buffer, v1.PatchOptions{})
	if err != nil {
		return nil, err
	}

	return patched, nil
}

func (sync *synchronizerPair) synchronize(api kubernetes.DynamicNamespaceableOperator, res *DynamicMutatedResult) (*unstructured.Unstructured, error) {
	if res.MutatedResource == nil {
		return nil, fmt.Errorf("sync a empty resource")
	}
	if res.OriginalResource == nil {
		return nil, fmt.Errorf("synchonrizing resource must specify the origin")
	}

	updated, err := api.Namespace(res.MutatedResource.GetNamespace()).Synchronize(context.Background(), res.OriginalResource,
		res.MutatedResource, v1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func (sync *synchronizerPair) patchStatus(api kubernetes.DynamicNamespaceableOperator, res *DynamicMutatedResult) (*unstructured.Unstructured, error) {
	if res.MutatedResource == nil || res.OriginalResource == nil {
		return nil, fmt.Errorf("patch a empty resource")
	}

	patch, err := jsondiff.Compare(res.OriginalResource, res.MutatedResource)
	if err != nil {
		return nil, err
	}

	buffer, err := json.Marshal(patch)
	if err != nil {
		return nil, err
	}

	patched, err := api.Namespace(res.MutatedResource.GetNamespace()).PatchStatus(context.Background(), res.MutatedResource.GetName(),
		types.JSONPatchType, buffer, v1.PatchOptions{})
	if err != nil {
		return nil, err
	}

	return patched, nil
}

func (sync *synchronizerPair) findSourceAssignment(namespace, name string) (*operationalAssignment, bool) {
	return findAssignment(sync.source.assignmentMap, namespace, name)
}

func (sync *synchronizerPair) findTargetAssignment(namespace, name string) (*operationalAssignment, bool) {
	return findAssignment(sync.target.assignmentMap, namespace, name)
}

func (sync *synchronizerPair) deleteAssignment(as *operationalAssignment) {
	deleteAssignment(sync.source.assignmentMap, as.GetNamespace(), as.GetName())

	deleteAssignment(sync.target.assignmentMap, as.mappingNamespace, as.mappingName)
}

func (sync *synchronizerPair) GetSourceInformerHandler() informer.DynamicEventHandler {
	return &utils.HandlerFunc{Creating: sync.onSourceAdd, Updating: sync.onSourceUpdate, Deleting: sync.onSourceDelete}
}

func (sync *synchronizerPair) GetTargetInformerHandler() informer.DynamicEventHandler {
	return &utils.HandlerFunc{Creating: sync.onTargetAdd, Updating: sync.onTargetUpdate, Deleting: sync.onTargetDelete}
}

func (sync *synchronizerPair) SetSourceResourceStore(store informer.UnstructuredStore) DynamicSynchronizerBuilder {
	sync.source.resourceStore = store
	return sync
}

func (sync *synchronizerPair) SetTargetResourceStore(store informer.UnstructuredStore) DynamicSynchronizerBuilder {
	sync.target.resourceStore = store
	return sync
}

func (sync *synchronizerPair) CreateSynchronizer() DynamicSynchronizer {
	return sync
}

func deleteAssignment(assignmentMap namespaceSelector, namespace, name string) {
	namedTask, ok := assignmentMap.Get(namespace)
	if !ok {
		return
	}

	_, ok = namedTask.Get(name)
	if !ok {
		return
	}
	namedTask.Remove(name)

	if namedTask.IsEmpty() {
		assignmentMap.Remove(namespace)
	}
}

func findAssignment(taskMap namespaceSelector, namespace, name string) (*operationalAssignment, bool) {
	namedTask, ok := taskMap.Get(namespace)
	if !ok {
		return nil, false
	}

	task, ok := namedTask.Get(name)
	if !ok {
		return nil, false
	}

	return task, true
}

func getResource(resourceMap map[string]map[string]*unstructured.Unstructured, namespace,
	name string) (*unstructured.Unstructured, bool) {
	var storedTargetResource *unstructured.Unstructured
	namespaceableMap, ok := resourceMap[namespace]
	if ok {
		storedTargetResource, ok = namespaceableMap[name]
	}
	return storedTargetResource, ok
}
