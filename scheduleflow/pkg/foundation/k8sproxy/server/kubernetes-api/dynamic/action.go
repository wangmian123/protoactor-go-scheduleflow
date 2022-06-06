package dynamic_api

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/k8sproxy"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

func (api *dynamicAPI) get(info *k8sproxy.Get) (interface{}, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}

	opt := info.GetOptions
	if opt == nil {
		opt = &metav1.GetOptions{}
	}

	resource, err := api.client.Resource(k8sproxy.ConvertGVR(*info.GVR)).Namespace(info.Metadata.Namespace).Get(
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

func (api *dynamicAPI) create(info *k8sproxy.Create) (interface{}, error) {
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

	resource, err = api.client.Resource(k8sproxy.ConvertGVR(*info.GVR)).Namespace(info.Metadata.Namespace).
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

func (api *dynamicAPI) delete(info *k8sproxy.Delete) (interface{}, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}

	opt := info.GetDeleteOptions()
	if opt == nil {
		opt = &metav1.DeleteOptions{}
	}

	err := api.client.Resource(k8sproxy.ConvertGVR(*info.GVR)).Namespace(info.Metadata.Namespace).
		Delete(context.Background(), info.Metadata.Name, *opt, info.SubResources...)
	if err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}

	return createKubernetesAPIResponse(info, nil), nil
}

func (api *dynamicAPI) update() (interface{}, error) {
	return nil, nil
}

func (api *dynamicAPI) list(info *k8sproxy.List) (interface{}, error) {
	if err := checkKubernetesBaseInformation(info); err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}

	opt := info.GetListOptions()
	if opt == nil {
		opt = &metav1.ListOptions{}
	}

	resourceList, err := api.client.Resource(k8sproxy.ConvertGVR(*info.GVR)).Namespace(info.Metadata.Namespace).
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

func (api *dynamicAPI) updateStatus() (interface{}, error) {
	return nil, nil
}

func (api *dynamicAPI) patch(info *k8sproxy.Patch) (interface{}, error) {

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

	patchType, ok := k8sproxy.PatchTypeMap[info.PatchType]
	if !ok {
		patchType = types.StrategicMergePatchType
	}

	resourceList, err := api.client.Resource(k8sproxy.ConvertGVR(*info.GVR)).Namespace(info.Metadata.Namespace).
		Patch(context.Background(), info.Metadata.Name, patchType, info.Resource, *opt, info.SubResources...)
	if err != nil {
		return createKubernetesAPIErrorResponse(err), nil
	}

	resp, err := json.Marshal(resourceList)
	if err != nil {
		return nil, err
	}
	return createKubernetesAPIResponse(info, resp), nil
}

func createKubernetesAPIResponse(res k8sproxy.KubernetesAPIBase, resource []byte) *k8sproxy.Response {
	return &k8sproxy.Response{
		Metadata: res.GetMetadata(),
		GVR:      res.GetGVR(),
		Resource: resource,
	}
}

func createKubernetesAPIErrorResponse(err error) *k8sproxy.Response {
	return &k8sproxy.Response{Error: &k8sproxy.Error{Message: err.Error()}}
}

func checkKubernetesBaseInformation(base k8sproxy.KubernetesAPIBase) error {
	if base.GetGVR() == nil {
		return fmt.Errorf("GroupVersionResource is nil")
	}

	if base.GetMetadata() == nil {
		return fmt.Errorf("Metadata is nil")
	}

	return nil
}
