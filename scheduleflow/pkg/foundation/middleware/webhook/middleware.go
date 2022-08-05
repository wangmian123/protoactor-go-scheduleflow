package webhook

import (
	"fmt"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/kubecore/webhook"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/utils"
	"github.com/sirupsen/logrus"
	admissionregistration "k8s.io/api/admissionregistration/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const webhookServer = "WebhookServer"

const registerTimeout = 30 * time.Second

type Registry interface {
	Register(webhookConfig admissionregistration.MutatingWebhook, handler admission.Handler) error
}

type registry struct {
	server *actor.PID
	root   *actor.RootContext
}

func (r *registry) Register(webhookConfig admissionregistration.MutatingWebhook, handler admission.Handler) error {
	info := &webhook.RegisterWebhook{
		Path:    *webhookConfig.ClientConfig.Service.Path,
		Handler: handler,
	}
	future := r.root.RequestFuture(r.server, info, registerTimeout)
	result, err := future.Result()
	if err != nil {
		return err
	}

	ack, ok := result.(*webhook.RegisterWebhookResult)
	if !ok {
		return fmt.Errorf("expected type *webhook.RegisterWebhookResult, but get %T", result)
	}

	if ack.Error != nil {
		return ack.Error
	}
	return nil
}

func newRegistry(sys *actor.ActorSystem, server *actor.PID) Registry {
	return &registry{root: sys.Root, server: server}
}

func NewServerMiddlewareProducer(kubeconfig *rest.Config, certDir string) actor.ReceiverMiddleware {
	started := func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) bool {
		pid := actor.NewPID(c.ActorSystem().Address(), webhookServer)
		_, ok := c.ActorSystem().ProcessRegistry.Get(pid)
		if ok {
			return false
		}
		_, err := c.ActorSystem().Root.SpawnNamed(webhook.New(kubeconfig, certDir), webhookServer)
		if err != nil {
			logrus.Fatalf("spawn webhook server with error %v", err)
		}
		return false
	}
	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(started).ProduceReceiverMiddleware()
}

func NewRegistryMiddlewareProducer() actor.ReceiverMiddleware {
	started := func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) bool {
		pid := actor.NewPID(c.ActorSystem().Address(), webhookServer)
		reg := newRegistry(c.ActorSystem(), pid)

		err := utils.InjectActor(c, utils.NewInjectorItem("", reg))
		if err != nil {
			logrus.Error(err)
		}
		return false
	}

	return utils.NewReceiverMiddlewareBuilder().BuildOnStarted(started).ProduceReceiverMiddleware()
}
