package utils

import (
	jsoniter "github.com/json-iterator/go"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var json jsoniter.API

func init() {
	json = jsoniter.ConfigCompatibleWithStandardLibrary
}

type NamespaceableResource interface {
	GetName() string
	GetNamespace() string
}

func ResourceListToMap[R NamespaceableResource](list []R) map[string]map[string]R {
	if len(list) == 0 {
		return make(map[string]map[string]R)
	}

	resourceMap := make(map[string]map[string]R)
	for _, res := range list {
		namespacedMap, ok := resourceMap[res.GetNamespace()]
		if !ok {
			namespacedMap = make(map[string]R)
			resourceMap[res.GetNamespace()] = namespacedMap
		}
		namespacedMap[res.GetName()] = res
	}

	return resourceMap
}

func ResourceConvert[I, O any](resource *I) (*O, error) {
	buffer, err := json.Marshal(resource)
	if err != nil {
		return nil, err
	}

	out := new(O)
	err = json.Unmarshal(buffer, out)
	if err != nil {
		return nil, err
	}

	return out, nil
}

type HandlerFunc struct {
	Creating func(resource unstructured.Unstructured)
	Deleting func(resource unstructured.Unstructured)
	Updating func(oldResource, newResource unstructured.Unstructured)
}

func (h *HandlerFunc) AddFunc(resource unstructured.Unstructured) {
	if h.Creating != nil {
		h.Creating(resource)
	}
}

func (h *HandlerFunc) DeleteFunc(resource unstructured.Unstructured) {
	if h.Deleting != nil {
		h.Deleting(resource)
	}
}

func (h *HandlerFunc) UpdateFunc(oldResource, newResource unstructured.Unstructured) {
	if h.Updating != nil {
		h.Updating(oldResource, newResource)
	}
}
