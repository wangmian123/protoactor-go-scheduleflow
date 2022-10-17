package webhook

import (
	"context"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/foundation/middleware/actorinfo"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

const logPrefix = "[WebhookManager]"

type webhookManager struct {
	actorinfo.ActorBaseInformation `inject:""`

	certDir    string
	kubeconfig *rest.Config
	server     *webhook.Server
}

func New(kubeconfig *rest.Config, certificateDir string) *actor.Props {
	goCtx, cancel := context.WithCancel(context.Background())
	return actor.PropsFromProducer(func() actor.Actor {
		return &webhookManager{
			kubeconfig: kubeconfig,
			certDir:    certificateDir,
		}
	}, actor.WithReceiverMiddleware(
		actorinfo.NewMiddlewareProducer(actorinfo.WithGoroutineContext(goCtx, cancel)),
	))
}

func (m *webhookManager) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *actor.Started:
		err := m.initialWebhook(ctx)
		if err != nil {
			logrus.Fatalf("%s initial webhook with error %v", logPrefix, err)
		}
	case *RegisterWebhook:
		m.server.Register(msg.Path, &webhook.Admission{Handler: msg.Handler})
		logrus.Infof("%s webhook %s registers success", logPrefix, msg.Path)
		ctx.Send(ctx.Sender(), &RegisterWebhookResult{})
	}
}

func (m *webhookManager) initialWebhook(ctx actor.Context) error {
	logrus.Infof("=======%s start=======", logPrefix)
	webhookMgr, err := manager.New(m.kubeconfig, manager.Options{
		Port:               9443,
		CertDir:            m.certDir,
		MetricsBindAddress: "0",
	})
	if err != nil {
		return err
	}

	hookServer := webhookMgr.GetWebhookServer()
	m.server = hookServer
	go func() {
		runtime.Must(webhookMgr.Start(m.Goroutine()))
	}()
	return nil
}
