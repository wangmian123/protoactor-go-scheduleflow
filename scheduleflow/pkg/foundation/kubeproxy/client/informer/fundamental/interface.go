package fundamental

import (
	"fmt"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
)

const (
	LogPrefix = "[InformerClient]"
)

var json jsoniter.API

func init() {
	json = jsoniter.ConfigCompatibleWithStandardLibrary
}

type SubscribeResoundCode int

const (
	Success = iota
	Fail
)

type SubscribeSourceInformation interface {
	SourcePID() *actor.PID
	SubscribeResource() *kubeproxy.SubscribeResource
}

type ResourceEventHandlerFuncs[R any] interface {
	AddFunc(resource R)
	DeleteFunc(resource R)
	UpdateFunc(oldResource, newResource R)
	Unmarshal([]byte) (R, error)
}

type SubscribeResourceFrom[R any] struct {
	Source   *actor.PID
	Resource *kubeproxy.SubscribeResource
	Handler  ResourceEventHandlerFuncs[R]
}

func (sub *SubscribeResourceFrom[R]) SourcePID() *actor.PID {
	return sub.Source
}

func (sub *SubscribeResourceFrom[R]) SubscribeResource() *kubeproxy.SubscribeResource {
	return sub.Resource
}

type SubscribeRespond struct {
	Code    SubscribeResoundCode
	Message interface{}
}

type EventKeyStruct struct {
	PIDGVRStruct
	*kubeproxy.SubscribeAction
}

func FormEventKeyString(pid *actor.PID, gvr *kubeproxy.GroupVersionResource, actionType kubeproxy.SubscribeAction) (string, error) {
	if pid == nil || gvr == nil {
		return "", fmt.Errorf("get a nil input")
	}
	key := EventKeyStruct{
		PIDGVRStruct: PIDGVRStruct{
			PID:                  pid,
			GroupVersionResource: gvr,
		},
		SubscribeAction: &actionType,
	}
	buffer, err := json.Marshal(key)
	if err != nil {
		return "", err
	}
	return string(buffer), nil
}

func FormEvenKeyStruct(key string) (*EventKeyStruct, error) {
	keyStruct := &EventKeyStruct{}
	err := json.Unmarshal([]byte(key), keyStruct)
	if err != nil {
		return nil, err
	}
	return keyStruct, nil
}

func FormSubscribeEventKeys(target *actor.PID, gvr *kubeproxy.GroupVersionResource, code int32) ([]string, error) {
	actionTypes := kubeproxy.GenerateSubscribeAction(code)
	keys := make([]string, 0, len(actionTypes))
	for _, actType := range actionTypes {
		key, err := FormEventKeyString(target, gvr, actType)
		if err != nil {
			logrus.Errorf("%s from key error %v", LogPrefix, err)
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

type PIDGVRStruct struct {
	*actor.PID
	*kubeproxy.GroupVersionResource
}

func FormPIDGVRKeyString(pid *actor.PID, gvr *kubeproxy.GroupVersionResource) (string, error) {
	buffer, err := json.Marshal(PIDGVRStruct{
		PID:                  pid,
		GroupVersionResource: gvr,
	})
	if err != nil {
		return "", fmt.Errorf("form key error")
	}
	return string(buffer), nil
}

func FormPIDGVRKeyStruct(key string) (*PIDGVRStruct, error) {
	keyStruct := &PIDGVRStruct{}
	err := json.Unmarshal([]byte(key), keyStruct)
	if err != nil {
		return nil, err
	}
	return keyStruct, nil
}
