package fundamental

import (
	"fmt"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
)

const (
	LogPrefix = "[InformerClient]"
)

type SubscribeResoundCode int

const (
	Success = iota
	Fail
)

type ResourceEventHandlerFuncs[R any] interface {
	AddFunc(resource R)
	DeleteFunc(resource R)
	UpdateFunc(oldResource, newResource R)
	Unmarshal([]byte) (R, error)
}

type SubscribeResourceFrom[R any] struct {
	Target   *actor.PID
	Resource kubeproxy.SubscribeResource
	Handler  ResourceEventHandlerFuncs[R]
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
