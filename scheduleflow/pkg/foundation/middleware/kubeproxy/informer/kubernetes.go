package informer

import (
	"github.com/asynkron/protoactor-go/actor"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Kubernetes Core: Pod

func FormPodKey(pod v1.Pod) string {
	return FormResourceKey(pod.ObjectMeta)
}

func PodMiddlewareProducer(target *actor.PID, opts ...Option) actor.ReceiverMiddleware {
	return PodMiddlewareProducerWithTagName("", target, opts...)
}

func PodMiddlewareProducerWithTagName(tagName string, target *actor.PID, opts ...Option) actor.ReceiverMiddleware {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	gvr := schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "pods",
	}

	resource := SubscribeResource{
		GVR:    gvr,
		Option: cfg.SubscribeOption,
	}
	producer := MiddlewareProducer[v1.Pod](&podStoreConstraint{}, tagName)
	return producer.ProduceSubscribeMiddleware(target, resource, nil)
}

// Kubernetes Core: Node

func FormNodeKey(node v1.Node) string {
	return FormResourceKey(node.ObjectMeta)
}

func NodeMiddlewareProducer(target *actor.PID, opts ...Option) actor.ReceiverMiddleware {
	return NodeMiddlewareProducerWithTagName("", target, opts...)
}

func NodeMiddlewareProducerWithTagName(tagName string, target *actor.PID, opts ...Option) actor.ReceiverMiddleware {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	gvr := schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "nodes",
	}

	resource := SubscribeResource{
		GVR:    gvr,
		Option: cfg.SubscribeOption,
	}
	producer := MiddlewareProducer[v1.Node](&nodeStoreConstraint{}, tagName)
	return producer.ProduceSubscribeMiddleware(target, resource, nil)
}
