package informer

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
)

type podConstraint struct {
}

func (po *podConstraint) FormKey(pod v1.Pod) string {
	return pod.String()
}

func (po *podConstraint) DeepCopy(pod v1.Pod) v1.Pod {
	return *pod.DeepCopy()
}

func TestNewK8sResourceSubscriber(t *testing.T) {
	podCons := &podConstraint{}
	a := newK8sResourceSubscribeWithStore[v1.Pod](podCons)
	fmt.Print(a)
}
