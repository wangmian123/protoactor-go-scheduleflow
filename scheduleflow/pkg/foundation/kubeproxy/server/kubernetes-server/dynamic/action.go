package dynamic_api

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

func (api *dynamicAPI) get(info *kubeproxy.Get) (interface{}, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}

	opt := info.GetOptions
	if opt == nil {
		opt = &metav1.GetOptions{}
	}

	resource, err := api.client.Resource(kubeproxy.ConvertGVR(*info.GVR)).Namespace(info.Metadata.Namespace).Get(
		context.Background(), info.Metadata.Namespace, *opt, info.SubResources...)
	if err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}

	resp, err := resource.MarshalJSON()
	if err != nil {
		return nil, err
	}

	return createKubernetesAPIResponse(info, resp), nil
}

func (api *dynamicAPI) create(info *kubeproxy.Create) (interface{}, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}

	if info.Resource == nil {
		err := fmt.Errorf("create resource is nil")
		return createKubernetesAPIErrorResponse(err), nil
	}

	resource := &unstructured.Unstructured{}
	err := json.Unmarshal(info.Resource, resource)
	if err != nil {
		return nil, err
	}

	opt := info.GetCreateOptions()
	if opt == nil {
		opt = &metav1.CreateOptions{}
	}

	resource, err = api.client.Resource(kubeproxy.ConvertGVR(*info.GVR)).Namespace(info.Metadata.Namespace).
		Create(context.Background(), resource, *opt, info.SubResources...)
	if err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}

	resp, err := resource.MarshalJSON()
	if err != nil {
		return nil, err
	}

	return createKubernetesAPIResponse(info, resp), nil
}

func (api *dynamicAPI) delete(info *kubeproxy.Delete) (interface{}, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}

	opt := info.GetDeleteOptions()
	if opt == nil {
		opt = &metav1.DeleteOptions{}
	}

	err := api.client.Resource(kubeproxy.ConvertGVR(*info.GVR)).Namespace(info.Metadata.Namespace).
		Delete(context.Background(), info.Metadata.Name, *opt, info.SubResources...)
	if err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}

	return createKubernetesAPIResponse(info, nil), nil
}

func (api *dynamicAPI) update(info *kubeproxy.Update) (interface{}, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}

	opt := info.GetUpdateOptions()
	if opt == nil {
		opt = &metav1.UpdateOptions{}
	}

	resource := &unstructured.Unstructured{}
	err := json.Unmarshal(info.Resource, resource)
	if err != nil {
		return nil, err
	}

	resource, err = api.client.Resource(kubeproxy.ConvertGVR(*info.GVR)).Namespace(info.Metadata.Namespace).
		Update(context.Background(), resource, *opt, info.SubResources...)

	if err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}
	resp, err := json.Marshal(resource)
	if err != nil {
		return nil, err
	}
	return createKubernetesAPIResponse(info, resp), nil
}

func (api *dynamicAPI) list(info *kubeproxy.List) (interface{}, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}

	opt := info.GetListOptions()
	if opt == nil {
		opt = &metav1.ListOptions{}
	}

	resourceList, err := api.client.Resource(kubeproxy.ConvertGVR(*info.GVR)).Namespace(info.Metadata.Namespace).
		List(context.Background(), *opt)

	if err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}

	resp, err := json.Marshal(resourceList)
	if err != nil {
		return nil, err
	}

	return createKubernetesAPIResponse(info, resp), nil
}

func (api *dynamicAPI) updateStatus(info *kubeproxy.UpdateStatus) (interface{}, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}

	opt := info.GetUpdateOptions()
	if opt == nil {
		opt = &metav1.UpdateOptions{}
	}

	resource := &unstructured.Unstructured{}
	err := json.Unmarshal(info.Resource, resource)
	if err != nil {
		return nil, err
	}

	resource, err = api.client.Resource(kubeproxy.ConvertGVR(*info.GVR)).Namespace(info.Metadata.Namespace).
		UpdateStatus(context.Background(), resource, *opt)

	if err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}
	resp, err := json.Marshal(resource)
	if err != nil {
		return nil, err
	}
	return createKubernetesAPIResponse(info, resp), nil
}

func (api *dynamicAPI) patch(info *kubeproxy.Patch) (interface{}, error) {

	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}

	if info.Resource == nil {
		err := fmt.Errorf("patch resource is nil")
		return createKubernetesAPIErrorResponse(err), nil
	}

	opt := info.GetPatchOptions()
	if opt == nil {
		opt = &metav1.PatchOptions{}
	}

	patchType, ok := kubeproxy.PatchCodeToType[info.PatchType]
	if !ok {
		patchType = types.StrategicMergePatchType
	}

	resource, err := api.client.Resource(kubeproxy.ConvertGVR(*info.GVR)).Namespace(info.Metadata.Namespace).
		Patch(context.Background(), info.Metadata.Name, patchType, info.Resource, *opt, info.SubResources...)
	if err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}

	resp, err := json.Marshal(resource)
	if err != nil {
		return nil, err
	}
	return createKubernetesAPIResponse(info, resp), nil
}

func createKubernetesAPIResponse(res kubeproxy.KubernetesAPIBase, resource []byte) *kubeproxy.Response {
	return &kubeproxy.Response{
		Metadata: res.GetMetadata(),
		GVR:      res.GetGVR(),
		Resource: resource,
	}
}

func createKubernetesAPIErrorResponse(err error) *kubeproxy.Response {
	return &kubeproxy.Response{Error: &kubeproxy.Error{Message: err.Error()}}
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
