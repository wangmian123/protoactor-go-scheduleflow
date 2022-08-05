package webhook

import (
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

type RegisterWebhook struct {
	Path    string
	Handler admission.Handler
}

type RegisterWebhookResult struct {
	Error error
}
