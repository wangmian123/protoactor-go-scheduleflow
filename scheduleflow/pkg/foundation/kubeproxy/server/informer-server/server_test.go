package informer_server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"

	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"

	"k8s.io/client-go/tools/cache"

	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime/schema"

	cmap "github.com/orcaman/concurrent-map"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	kubeconfig "sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/asynkron/protoactor-go/actor"
)

type testDynamicInformerHandler struct {
	formKey func(any) string
	store   cmap.ConcurrentMap[any]
}

func newTestHandler(formKey func(any) string) cache.ResourceEventHandlerFuncs {
	handler := &testDynamicInformerHandler{
		formKey: formKey,
		store:   cmap.New[any](),
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    handler.addFunc,
		UpdateFunc: handler.updateFunc,
		DeleteFunc: handler.deleteFunc,
	}
}

func (t *testDynamicInformerHandler) addFunc(obj any) {
	key := t.formKey(obj)
	logrus.Infof("receive add message: key %s", key)
	t.store.Set(key, obj)
}

func (t *testDynamicInformerHandler) updateFunc(_, obj2 any) {
	key := t.formKey(obj2)
	logrus.Infof("receive update message: key %s", key)
	t.store.Set(key, obj2)
}

func (t *testDynamicInformerHandler) deleteFunc(obj any) {
	key := t.formKey(obj)
	logrus.Infof("receive delete message: key %s", key)
	t.store.Remove(key)
}

func TestInformerServer_Receive(t *testing.T) {
	//pod := schema.GroupVersionResource{
	//	Group:    "",
	//	Version:  "v1",
	//	Resource: "pods",
	//}

	nodes := schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "nodes",
	}
	//
	//cluster := schema.GroupVersionResource{
	//	Group:    "multicluster.scheduleflow.io",
	//	Version:  "v1alpha1",
	//	Resource: "clusterproxies",
	//}

	system := actor.NewActorSystem()
	kubecfg, _ := kubeconfig.GetConfig()
	// uses the current context in kubeconfig
	// path-to-kubeconfig -- for example, /root/.kube/config
	// creates the clientset
	clientset, _ := kubernetes.NewForConfig(kubecfg)
	// access the API to list pods
	pods, _ := clientset.CoreV1().Pods("").List(context.TODO(), v1.ListOptions{})
	fmt.Printf("There are %d pods in the cluster\n", len(pods.Items))

	pid := system.Root.Spawn(New(kubecfg))

	res := &kubeproxy.SubscribeResource{GVR: kubeproxy.NewGroupVersionResource(nodes)}
	receive := func(ctx actor.Context) {
		switch msg := ctx.Message().(type) {
		case *actor.Started:
			ctx.Request(pid, res)
		default:
			logrus.Infof("receive message type %T", msg)
		}
	}

	system.Root.Spawn(actor.PropsFromFunc(receive))

	time.Sleep(100 * time.Second)
}

func testDynamic(gvr schema.GroupVersionResource, kubecfg *rest.Config) {
	dyClient := dynamic.NewForConfigOrDie(kubecfg)
	dyFactory := dynamicinformer.NewDynamicSharedInformerFactory(dyClient, 30*time.Second)
	formKey := func(obj any) string {
		res, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return ""
		}
		return res.GetNamespace() + "/" + res.GetName()
	}
	dyFactory.ForResource(gvr).Informer().AddEventHandler(newTestHandler(formKey))
	stop := make(chan struct{})
	dyFactory.Start(stop)
}
