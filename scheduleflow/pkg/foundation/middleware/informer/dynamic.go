package informer

import (
	"fmt"
	"strings"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubeproxy/client/informer/fundamental"
	regexp "github.com/dlclark/regexp2"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var compiled *regexp.Regexp

func init() {
	var err error
	compiled, err = regexp.Compile("\\{( *)\\}", regexp.None)
	if err != nil {
		logrus.Fatalf("can not compile {( *)}, due to %v", err)
	}
}

type DynamicSubscribe struct {
	Target          *actor.PID
	Resource        SubscribeResource
	StoreConstraint StoreConstraint[unstructured.Unstructured]
	Handler         DynamicEventHandler
	UpdateInterval  time.Duration
}

func WithDynamicStoreConstraint(store StoreConstraint[unstructured.Unstructured]) Option {
	return func(c *config) {
		c.storeConstraint = store
	}
}

func WithDynamicInformerHandler(handler DynamicEventHandler) Option {
	return func(c *config) {
		c.handler = handler
	}
}

func NewDynamicSubscribe(target *actor.PID, gvr schema.GroupVersionResource, opts ...Option) DynamicSubscribe {
	cfg := &config{}
	cfg.storeConstraint = &defaultConstraint{}

	for _, opt := range opts {
		opt(cfg)
	}

	return DynamicSubscribe{
		Target: target,
		Resource: SubscribeResource{
			GVR:    gvr,
			Option: cfg.SubscribeOption,
		},
		StoreConstraint: cfg.storeConstraint,
		Handler:         cfg.handler,
		UpdateInterval:  cfg.updateInterval,
	}
}

type dynamicHandlerConverter struct {
	handler DynamicEventHandler
}

func ConvertDynamicEventHandler(handler DynamicEventHandler) fundamental.ResourceEventHandlerFuncs[unstructured.Unstructured] {
	return &dynamicHandlerConverter{
		handler: handler,
	}
}

func (converter *dynamicHandlerConverter) AddFunc(resource unstructured.Unstructured) {
	if converter.handler == nil {
		return
	}
	converter.handler.AddFunc(resource)
}

func (converter *dynamicHandlerConverter) DeleteFunc(resource unstructured.Unstructured) {
	if converter.handler == nil {
		return
	}
	converter.handler.DeleteFunc(resource)
}

func (converter *dynamicHandlerConverter) UpdateFunc(oldResource, newResource unstructured.Unstructured) {
	if converter.handler == nil {
		return
	}
	converter.handler.UpdateFunc(oldResource, newResource)
}

func (converter *dynamicHandlerConverter) Unmarshal(data []byte) (unstructured.Unstructured, error) {
	converted := unstructured.Unstructured{}
	err := json.Unmarshal(data, &converted)
	return converted, err
}

func formGVRKey(pid *actor.PID, gvr schema.GroupVersionResource) string {
	return pid.Address + "/" + pid.Id + "." + gvr.Group + "." + gvr.Version + "." + gvr.Resource
}

type dynamicInformer struct {
	system      *actor.ActorSystem
	informerPid *actor.PID
	gvrMap      cmap.ConcurrentMap[*k8sOperableResourceStore[unstructured.Unstructured]]
}

func NewDynamicInformer(sys *actor.ActorSystem, informerPid *actor.PID) DynamicInformer {
	return &dynamicInformer{
		system:      sys,
		informerPid: informerPid,
		gvrMap:      cmap.New[*k8sOperableResourceStore[unstructured.Unstructured]](),
	}
}

func (dy *dynamicInformer) SetResourceHandler(subscribe ...DynamicSubscribe) ([]UnstructuredOperableStore, error) {
	if subscribe == nil || dy.system == nil {
		return nil, fmt.Errorf("%s inputs are empty", logPrefix)
	}

	if dy.informerPid == nil {
		informerPid := actor.NewPID(dy.system.Address(), informerClient)
		_, ok := dy.system.ProcessRegistry.Get(informerPid)
		if !ok {
			var err error
			logrus.Warningf("infomer client dose not started when actor start")
			informerPid, err = dy.system.Root.SpawnNamed(informer.New(), informerClient)
			if err != nil {
				logrus.Fatalf("initial informer client fail due to %v", err)
			}
		}
		dy.informerPid = informerPid
	}

	resourceStores := make([]UnstructuredOperableStore, 0, len(subscribe))

	for _, subInfo := range subscribe {
		resourceStore, err := dy.subscribeToClient(subInfo)
		if err != nil {
			return nil, err
		}
		resourceStores = append(resourceStores, resourceStore)
	}
	return resourceStores, nil
}

func (dy *dynamicInformer) subscribeToClient(subInfo DynamicSubscribe) (*k8sOperableResourceStore[unstructured.Unstructured], error) {
	resourceStore, ok := dy.gvrMap.Get(formGVRKey(subInfo.Target, subInfo.Resource.GVR))
	if !ok {
		return dy.subscribeNewResource(subInfo)
	}

	err := dy.renewSubscribedResource(subInfo)
	if err != nil {
		return nil, err
	}
	return resourceStore, nil
}

func (dy *dynamicInformer) subscribeNewResource(subInfo DynamicSubscribe) (*dynamicStore, error) {
	resourceStore := newOperableResourceStore[unstructured.Unstructured](subInfo.StoreConstraint, subInfo.UpdateInterval)
	resourceStore.handlers = []fundamental.ResourceEventHandlerFuncs[unstructured.Unstructured]{ConvertDynamicEventHandler(subInfo.Handler)}
	subscribeEvent := &fundamental.SubscribeResourceFrom[unstructured.Unstructured]{
		Source:   GetPID(subInfo.Target.GetAddress()),
		Resource: convertSubscribeResource(subInfo.Resource),
		Handler:  resourceStore,
	}

	err := dy.sendSubscribeResource(subscribeEvent)
	if err != nil {
		return nil, err
	}
	dy.gvrMap.Set(formGVRKey(subInfo.Target, subInfo.Resource.GVR), resourceStore)
	return resourceStore, nil
}

func (dy *dynamicInformer) renewSubscribedResource(subInfo DynamicSubscribe) error {
	subscribeEvent := &fundamental.SubscribeResourceFrom[unstructured.Unstructured]{
		Source:   GetPID(subInfo.Target.GetAddress()),
		Resource: convertSubscribeResource(subInfo.Resource),
		Handler:  ConvertDynamicEventHandler(subInfo.Handler),
	}
	err := dy.sendSubscribeResource(subscribeEvent)
	if err != nil {
		return err
	}
	return nil
}

func (dy *dynamicInformer) sendSubscribeResource(subscribeEvent *fundamental.SubscribeResourceFrom[unstructured.Unstructured]) error {
	future := dy.system.Root.RequestFuture(dy.informerPid, subscribeEvent, timeout)
	result, err := future.Result()
	if err != nil {
		return fmt.Errorf("subscribe error due to %v", err)
	}

	ack, ok := result.(*fundamental.SubscribeRespond)
	if !ok {
		return fmt.Errorf("subscribe respond type expect *fundamental.SubscribeRespond, but get %T", result)
	}

	if ack.Code != fundamental.Success {
		return fmt.Errorf("subcribe event with error %s", ack.Message)
	}
	return nil
}

func (dy *dynamicInformer) GetResourceStore(target *actor.PID, gvr schema.GroupVersionResource) (KubernetesResourceStore[unstructured.Unstructured], bool) {
	return dy.gvrMap.Get(formGVRKey(target, gvr))
}

func ConvertToUnstructured[R any](res *R) (*unstructured.Unstructured, error) {
	if res == nil {
		return nil, nil
	}

	buffer, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}

	raw := new(unstructured.Unstructured)
	err = json.Unmarshal(buffer, raw)
	return raw, err
}

func ConvertToResource[R any](res *unstructured.Unstructured) (*R, error) {
	if res == nil {
		return nil, nil
	}

	buffer, err := res.MarshalJSON()
	if err != nil {
		return nil, err
	}

	raw := new(R)
	err = json.Unmarshal(buffer, raw)
	if err != nil {
		var trimErr error
		buffer, trimErr = trimAllEmptyStructure(buffer)
		if trimErr != nil {
			return nil, fmt.Errorf("marshal error %v, trim error %v", err, trimErr)
		}

		raw = new(R)
		err = json.Unmarshal(buffer, raw)
		if err != nil {
			return nil, fmt.Errorf("marshal error after trimming %v", err)
		}
	}
	return raw, nil
}

func trimAllEmptyStructure(buffer []byte) ([]byte, error) {
	converted := string(buffer)
	for true {
		matched, err := compiled.FindStringMatch(converted)
		if err != nil {
			return nil, err
		}
		if matched == nil {
			break
		}

		converted = strings.Replace(converted, matched.String(), "null", -1)
	}
	return []byte(converted), nil
}
