package fundamental

import (
	"fmt"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/sirupsen/logrus"
)

const (
	LogPrefix = "[InformerClient]"
)

type SubscribeResoundCode int

const (
	Success = iota
	Fail
)

type SubscribeSourceInformation interface {
	SourcePID() *actor.PID
	SubscribeResource() kubeproxy.SubscribeResource
}

type ResourceEventHandlerFuncs[R any] interface {
	AddFunc(resource R)
	DeleteFunc(resource R)
	UpdateFunc(oldResource, newResource R)
	Unmarshal([]byte) (R, error)
}

type SubscribeResourceFrom[R any] struct {
	Source   *actor.PID
	Resource kubeproxy.SubscribeResource
	Handler  ResourceEventHandlerFuncs[R]
}

func (sub *SubscribeResourceFrom[R]) SourcePID() *actor.PID {
	return sub.Source
}

func (sub *SubscribeResourceFrom[R]) SubscribeResource() kubeproxy.SubscribeResource {
	return sub.Resource
}

type SubscribeRespond struct {
	Code    SubscribeResoundCode
	Message interface{}
}

func FormKey(pid *actor.PID, gvr *kubeproxy.GroupVersionResource, actionType kubeproxy.SubscribeAction) (string, error) {
	if pid == nil || gvr == nil {
		return "", fmt.Errorf("get a nil input")
	}

	actionName, ok := kubeproxy.SubscribeAction_name[int32(actionType)]
	if !ok {
		return "", fmt.Errorf("SubscribeAction not exist")
	}
	pidName := fmt.Sprintf("%s/%s", pid.Address, pid.Id)
	gvrName := fmt.Sprintf("%s.%s.%s", gvr.Group, gvr.Version, gvr.Resource)

	return fmt.Sprintf("%s-%s-%s", pidName, gvrName, actionName), nil

}

func FormSubscriberKeys(target *actor.PID, gvr *kubeproxy.GroupVersionResource, code int32) ([]string, error) {
	actionTypes := kubeproxy.GenerateSubscribeAction(code)

	keys := make([]string, 0, len(actionTypes))
	for _, actType := range actionTypes {
		key, err := FormKey(target, gvr, actType)
		if err != nil {
			logrus.Errorf("%s from key error %v", LogPrefix, err)
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}
