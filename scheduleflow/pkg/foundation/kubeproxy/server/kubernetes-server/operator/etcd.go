package operator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	jsonpatch "github.com/evanphx/json-patch"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"github.com/wI2L/jsondiff"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

var json jsoniter.API

func init() {
	json = jsoniter.ConfigCompatibleWithStandardLibrary
}

func (ope *resourceOperator) create(info *kubeproxy.Create) (*kubeproxy.Response, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}

	if info.Resource == nil {
		err := fmt.Errorf("create resource is nil")
		return createKubernetesAPIErrorResponse(info, err), nil
	}

	resource := &unstructured.Unstructured{}
	err := json.Unmarshal(info.Resource, resource)
	if err != nil {
		return nil, err
	}

	opt := info.GetCreateOptions()
	if opt == nil {
		opt = &v1.CreateOptions{}
	}

	resource, err = ope.Resource(ope.gvr).Namespace(info.Metadata.Namespace).
		Create(context.Background(), resource, *opt, info.SubResources...)
	if err != nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}

	resp, err := resource.MarshalJSON()
	if err != nil {
		return nil, err
	}

	return createKubernetesAPIResponse(info, resp), nil
}

func (ope *resourceOperator) delete(info *kubeproxy.Delete) (*kubeproxy.Response, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}

	opt := info.GetDeleteOptions()
	if opt == nil {
		opt = &v1.DeleteOptions{}
	}

	err := ope.Resource(ope.gvr).Namespace(info.Metadata.Namespace).
		Delete(context.Background(), info.Metadata.Name, *opt, info.SubResources...)
	if err != nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}

	return createKubernetesAPIResponse(info, nil), nil
}

func (ope *resourceOperator) get(info *kubeproxy.Get) (*kubeproxy.Response, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}

	opt := info.GetOptions
	if opt == nil {
		opt = &v1.GetOptions{}
	}

	resource := &unstructured.Unstructured{Object: make(map[string]interface{})}
	resource.SetNamespace(info.GetMetadata().Namespace)
	resource.SetName(info.GetMetadata().Name)

	cond, ok := ope.updateLocker.Get(formResourceKey(resource))
	if ok {
		cond.resourceLock.Lock()
		defer cond.resourceLock.Unlock()
	}

	res, ok := ope.store.Get(*resource)
	if ok {
		resource = &res
	} else {
		var err error
		resource, err = ope.Resource(ope.gvr).Namespace(info.Metadata.Namespace).
			Get(context.Background(), info.Metadata.Name, *opt, info.SubResources...)

		if err != nil {
			return createKubernetesAPIErrorResponse(info, err), nil
		}
	}

	resp, err := resource.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return createKubernetesAPIResponse(info, resp), nil
}

func (ope *resourceOperator) blockGet(info *kubeproxy.BlockGet) (*kubeproxy.Response, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}

	resource := &unstructured.Unstructured{Object: make(map[string]interface{})}
	resource.SetNamespace(info.GetMetadata().Namespace)
	resource.SetName(info.GetMetadata().Name)

	cond := ope.createResourceCond(resource)
	cond.resourceLock.Lock()
	defer cond.resourceLock.Unlock()

	opt := kubeproxy.ConcertGetOption(info.GetGetOptions())
	var err error

	res, ok := ope.store.Get(*resource)
	if ok {
		resource = &res
	} else {
		resource, err = ope.Resource(ope.gvr).Namespace(info.Metadata.Namespace).
			Get(context.Background(), info.Metadata.Name, opt, info.SubResources...)

		if err != nil {
			return createKubernetesAPIErrorResponse(info, err), nil
		}
	}

	if err != nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}
	waitUntilCondition(cond.Cond, info.GetOptions.BlockTimeout.AsDuration())
	resp, err := resource.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return createKubernetesAPIResponse(info, resp), nil
}

func (ope *resourceOperator) list(info *kubeproxy.List) (*kubeproxy.Response, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}

	var resourceList *unstructured.UnstructuredList
	list := ope.store.List()
	if len(list) != 0 {
		resourceList = makeResourceList(list)
	} else {
		opt := info.GetListOptions()
		if opt == nil {
			opt = &v1.ListOptions{}
		}

		var err error
		resourceList, err = ope.Resource(ope.gvr).Namespace(info.Metadata.Namespace).List(context.Background(), *opt)
		if err != nil {
			return createKubernetesAPIErrorResponse(info, err), nil
		}
	}

	resp, err := json.Marshal(resourceList)
	if err != nil {
		return nil, err
	}

	return createKubernetesAPIResponse(info, resp), nil
}

func (ope *resourceOperator) update(info *kubeproxy.Update) (*kubeproxy.Response, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}
	if info.Resource == nil {
		err := fmt.Errorf("patch resource is nil")
		return createKubernetesAPIErrorResponse(info, err), nil
	}

	resource := &unstructured.Unstructured{}
	err := json.Unmarshal(info.Resource, resource)
	if err != nil {
		return nil, err
	}

	opt := info.GetUpdateOptions()
	if opt == nil {
		opt = &v1.UpdateOptions{}
	}
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		newest, err := ope.Resource(ope.gvr).Namespace(resource.GetNamespace()).
			Get(context.Background(), resource.GetName(), v1.GetOptions{})
		if err != nil {
			return err
		}

		resource.SetResourceVersion(newest.GetResourceVersion())

		updated, err := ope.Resource(ope.gvr).Namespace(info.Metadata.Namespace).
			Update(context.Background(), resource, *opt, info.SubResources...)

		if err != nil {
			return err
		}

		resource = updated
		return nil
	})

	cond := ope.createResourceCond(resource)
	cond.Broadcast()
	if err != nil || resource == nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}
	resp, err := json.Marshal(resource)
	if err != nil {
		return nil, err
	}
	return createKubernetesAPIResponse(info, resp), nil
}

func (ope *resourceOperator) updateStatus(info *kubeproxy.UpdateStatus) (*kubeproxy.Response, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}

	if info.Resource == nil {
		err := fmt.Errorf("patch resource is nil")
		return createKubernetesAPIErrorResponse(info, err), nil
	}

	resource := &unstructured.Unstructured{}
	err := json.Unmarshal(info.Resource, resource)
	if err != nil {
		return nil, err
	}

	opt := info.GetUpdateOptions()
	if opt == nil {
		opt = &v1.UpdateOptions{}
	}

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		newest, err := ope.Resource(ope.gvr).Namespace(resource.GetNamespace()).
			Get(context.Background(), resource.GetName(), v1.GetOptions{})
		if err != nil {
			return err
		}

		resource.SetResourceVersion(newest.GetResourceVersion())
		updated, err := ope.Resource(ope.gvr).Namespace(info.Metadata.Namespace).
			UpdateStatus(context.Background(), resource, *opt)
		if err != nil {
			return err
		}
		resource = updated
		return nil
	})
	cond := ope.createResourceCond(resource)
	cond.Broadcast()
	if err != nil || resource == nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}
	resp, err := json.Marshal(resource)
	if err != nil {
		return nil, err
	}
	return createKubernetesAPIResponse(info, resp), nil
}

func (ope *resourceOperator) patch(info *kubeproxy.Patch) (*kubeproxy.Response, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}

	if info.Resource == nil {
		err := fmt.Errorf("patch resource is nil")
		return createKubernetesAPIErrorResponse(info, err), nil
	}

	opt := info.GetPatchOptions()
	if opt == nil {
		opt = &v1.PatchOptions{}
	}
	patchType, ok := kubeproxy.PatchCodeToType[info.PatchType]
	if !ok {
		patchType = types.JSONPatchType
	}
	resource, err := ope.Resource(ope.gvr).Namespace(info.Metadata.Namespace).
		Patch(context.Background(), info.Metadata.Name, patchType, info.Resource, *opt, info.SubResources...)
	if err != nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}
	resp, err := json.Marshal(resource)
	if err != nil {
		return nil, err
	}
	return createKubernetesAPIResponse(info, resp), nil
}

func (ope *resourceOperator) patchStatus(info *kubeproxy.PatchStatus) (*kubeproxy.Response, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}

	if info.PatchType != 0 {
		return createKubernetesAPIErrorResponse(info, fmt.Errorf("only support patch type PatchType_JSONPatchType")), nil
	}

	if info.Resource == nil {
		err := fmt.Errorf("patch resource is nil")
		return createKubernetesAPIErrorResponse(info, err), nil
	}

	newest, err := ope.Resource(ope.gvr).Namespace(info.GetMetadata().Namespace).Get(context.Background(),
		info.GetMetadata().Name, v1.GetOptions{})

	modified, err := patchResource(info.Resource, newest)
	if err != nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}
	modified.SetResourceVersion(newest.GetResourceVersion())

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		newest, err = ope.Resource(ope.gvr).Namespace(modified.GetNamespace()).
			Get(context.Background(), modified.GetName(), v1.GetOptions{})
		if err != nil {
			return err
		}
		modified, err = patchResource(info.Resource, newest)
		if err != nil {
			return err
		}
		modified.SetResourceVersion(newest.GetResourceVersion())
		updated, err := ope.Resource(ope.gvr).Namespace(info.Metadata.Namespace).UpdateStatus(context.Background(), modified, v1.UpdateOptions{})
		if err != nil {
			return err
		}
		modified = updated
		return nil
	})

	if err != nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}
	resp, err := json.Marshal(modified)
	if err != nil {
		return nil, err
	}
	return createKubernetesAPIResponse(info, resp), nil
}

func (ope *resourceOperator) synchronize(info *kubeproxy.Synchronize) (*kubeproxy.Response, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}

	if len(info.Original) == 0 || len(info.Synchronizing) == 0 {
		err := fmt.Errorf("synchonrize resource is empty")
		return createKubernetesAPIErrorResponse(info, err), nil
	}

	origin := &unstructured.Unstructured{}
	err := json.Unmarshal(info.Original, origin)
	if err != nil {
		return nil, err
	}
	synchronizing := &unstructured.Unstructured{}
	err = json.Unmarshal(info.Synchronizing, synchronizing)
	if err != nil {
		return nil, err
	}

	updated := &unstructured.Unstructured{}

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		patchJson, err := jsondiff.Compare(origin, synchronizing)
		if err != nil {
			return err
		}

		patchBuffer, err := json.Marshal(patchJson)
		if err != nil {
			return err
		}

		resource, err := ope.Resource(ope.gvr).Namespace(info.Metadata.Namespace).
			Patch(context.Background(), info.Metadata.Name, types.JSONPatchType, patchBuffer, v1.PatchOptions{}, info.SubResources...)
		if err != nil {
			return err
		}
		modified, err := patchResource(patchBuffer, resource)
		if err != nil {
			return err
		}
		modified.SetResourceVersion(resource.GetResourceVersion())
		updated, err = ope.Resource(ope.gvr).Namespace(info.Metadata.Namespace).UpdateStatus(context.Background(), modified, v1.UpdateOptions{})
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return createKubernetesAPIErrorResponse(info, err), nil
	}
	resp, err := json.Marshal(updated)
	if err != nil {
		return nil, err
	}
	return createKubernetesAPIResponse(info, resp), nil
}

func (ope *resourceOperator) createResourceCond(resource *unstructured.Unstructured) *resourceCond {
	key := formResourceKey(resource)
	cond, ok := ope.updateLocker.Get(key)
	if !ok {
		cond = newResourceLock()
		ope.updateLocker.Set(key, cond)
	}

	return cond
}

func (ope *resourceOperator) patchToNewest(origin, updating *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	if updating.GetResourceVersion() != origin.GetResourceVersion() {
		return nil, fmt.Errorf("origin resource version should be the same as updating resource version")
	}

	newest, err := ope.Resource(ope.gvr).Namespace(updating.GetNamespace()).Get(context.Background(), updating.GetName(),
		v1.GetOptions{})
	if err != nil {
		return nil, err
	}

	if updating.GetResourceVersion() != newest.GetResourceVersion() {
		logrus.Infof("updating resource version %s, newest version %s", updating.GetResourceVersion(), newest.GetResourceVersion())
	}

	patchJson, err := jsondiff.Compare(origin, updating)
	if err != nil {
		return nil, err
	}

	patchBuffer, err := json.Marshal(patchJson)
	if err != nil {
		return nil, err
	}

	return patchResource(patchBuffer, newest)
}

func patchResource(patchBuffer []byte, resource *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	patch, err := jsonpatch.DecodePatch(patchBuffer)
	if err != nil {
		return nil, err
	}

	newestJson, err := resource.MarshalJSON()
	if err != nil {
		return nil, err
	}

	modified, err := patch.Apply(newestJson)
	if err != nil {
		return nil, err
	}

	appliedToResource := &unstructured.Unstructured{}

	err = json.Unmarshal(modified, appliedToResource)
	if err != nil {
		return nil, err
	}
	return appliedToResource, nil
}

func createKubernetesAPIResponse(res kubeproxy.KubernetesAPIBase, resource []byte) *kubeproxy.Response {
	return &kubeproxy.Response{
		Metadata: res.GetMetadata(),
		GVR:      res.GetGVR(),
		Resource: resource,
	}
}

func createKubernetesAPIErrorResponse(info kubeproxy.KubernetesAPIBase, err error) *kubeproxy.Response {
	return &kubeproxy.Response{
		Metadata: info.GetMetadata(),
		GVR:      info.GetGVR(),
		Error:    &kubeproxy.Error{Message: err.Error()},
	}
}

func checkKubernetesBaseInformation(base kubeproxy.KubernetesAPIBase) error {
	if base.GetGVR() == nil {
		return fmt.Errorf("GroupVersionResource is nil")
	}

	if base.GetMetadata() == nil {
		return fmt.Errorf("Metadata is nil")
	}

	return nil
}

func waitUntilCondition(cond *sync.Cond, timeout time.Duration) {
	if cond == nil {
		return
	}

	cond.L.Lock()
	defer cond.L.Unlock()

	stopWait := make(chan struct{})
	timer := time.After(timeout)
	go func() {
		for {
			select {
			case <-timer:
				cond.Broadcast()
			case <-stopWait:
				return
			}
		}
	}()
	cond.Wait()
	stopWait <- struct{}{}
}

func makeResourceList(list []unstructured.Unstructured) *unstructured.UnstructuredList {
	if len(list) == 0 {
		return nil
	}
	resourceList := &unstructured.UnstructuredList{
		Items: list,
	}

	resourceList.SetKind("List")
	resourceList.SetAPIVersion("v1")
	return resourceList
}
