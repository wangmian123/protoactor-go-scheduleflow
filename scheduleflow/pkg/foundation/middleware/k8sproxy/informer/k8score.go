package informer

import (
	"github.com/asynkron/protoactor-go/actor"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DeepCopier[R any] interface {
	DeepCopy() R
}

func DeepCopy[R any](source DeepCopier[R]) R {
	return source.DeepCopy()
}

func FormResourceKey(obj metav1.ObjectMeta) string {
	return obj.Namespace + "/" + obj.Namespace
}

// Kubernetes Core: Pod

func FormPodKey(pod v1.Pod) string {
	return FormResourceKey(pod.ObjectMeta)
}

type podStoreConstraint struct {
}

func (*podStoreConstraint) FormKey(pod v1.Pod) string {
	return FormPodKey(pod)
}

func (*podStoreConstraint) DeepCopy(pod v1.Pod) v1.Pod {
	return *DeepCopy[*v1.Pod](&pod)
}

func PodMiddlewareProducer(target *actor.PID, resource SubscribeResource) actor.ReceiverMiddleware {
	return PodMiddlewareProducerWithTagName("", target, resource)
}

func PodMiddlewareProducerWithTagName(tagName string, target *actor.PID, resource SubscribeResource) actor.ReceiverMiddleware {
	producer := MiddlewareProducer[v1.Pod](&podStoreConstraint{}, tagName)
	return producer.ProduceSubscribeMiddleware(target, resource, nil)
}

// Kubernetes Core: Node

func FormNodeKey(node v1.Node) string {
	return FormResourceKey(node.ObjectMeta)
}

type nodeStoreConstraint struct {
}

func (*nodeStoreConstraint) FormKey(node v1.Node) string {
	return FormNodeKey(node)
}

func (*nodeStoreConstraint) DeepCopy(node v1.Node) v1.Node {
	return *DeepCopy[*v1.Node](&node)
}

func NodeMiddlewareProducer(target *actor.PID, resource SubscribeResource) actor.ReceiverMiddleware {
	return NodeMiddlewareProducerWithTagName("", target, resource)
}

func NodeMiddlewareProducerWithTagName(tagName string, target *actor.PID, resource SubscribeResource) actor.ReceiverMiddleware {
	producer := MiddlewareProducer[v1.Node](&nodeStoreConstraint{}, tagName)
	return producer.ProduceSubscribeMiddleware(target, resource, nil)
}
