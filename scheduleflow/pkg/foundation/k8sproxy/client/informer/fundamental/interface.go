package fundamental

import (
	"fmt"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/k8sproxy"
)

const (
	ModuleName = "ClusterInformerClient"
	LogPrefix  = "[ClusterInformerClient]"
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
	Resource k8sproxy.SubscribeResource
	Handler  ResourceEventHandlerFuncs[R]
}

type SubscribeRespond struct {
	Code    SubscribeResoundCode
	Message interface{}
}

func FormKey(pid *actor.PID, gvr *k8sproxy.GroupVersionResource, actionType k8sproxy.SubscribeAction) (string, error) {
	if pid == nil || gvr == nil {
		return "", fmt.Errorf("get a nil input")
	}

	actionName, ok := k8sproxy.SubscribeAction_name[int32(actionType)]
	if !ok {
		return "", fmt.Errorf("SubscribeAction not exist")
	}
	pidName := fmt.Sprintf("%s/%s", pid.Address, pid.Id)
	gvrName := fmt.Sprintf("%s.%s.%s", gvr.Group, gvr.Version, gvr.Resource)

	return fmt.Sprintf("%s-%s-%s", pidName, gvrName, actionName), nil

}
