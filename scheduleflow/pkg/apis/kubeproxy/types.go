package kubeproxy

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/runtime/protoimpl"
	"google.golang.org/protobuf/types/known/durationpb"

	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

var PatchCodeToType map[PatchType]types.PatchType
var PatchTypeToCode map[types.PatchType]PatchType

func init() {
	PatchCodeToType = map[PatchType]types.PatchType{
		PatchType_JSONPatchType:           types.JSONPatchType,
		PatchType_ApplyPatchType:          types.ApplyPatchType,
		PatchType_MergePathType:           types.MergePatchType,
		PatchType_StrategicMergePatchType: types.StrategicMergePatchType,
	}

	PatchTypeToCode = map[types.PatchType]PatchType{
		types.JSONPatchType:           PatchType_JSONPatchType,
		types.ApplyPatchType:          PatchType_ApplyPatchType,
		types.MergePatchType:          PatchType_MergePathType,
		types.StrategicMergePatchType: PatchType_StrategicMergePatchType,
	}
}

type BlockGetOptions struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Add Manually
	v1.TypeMeta `json:",inline"`

	ResourceVersion string               `protobuf:"bytes,1,opt,name=ResourceVersion,proto3" json:"ResourceVersion,omitempty"`
	BlockTimeout    *durationpb.Duration `protobuf:"bytes,2,opt,name=BlockTimeout,proto3" json:"BlockTimeout,omitempty"`
}

type KubernetesAPIBase interface {
	GetMetadata() *v1.ObjectMeta
	GetGVR() *GroupVersionResource
}

type subscribeCfg struct {
	actionCode   int32
	subscribeOpt *SubscribeOption
}

type Option func(cfg *subscribeCfg)

func WithSubscribeAction(actions ...SubscribeAction) Option {
	return func(cfg *subscribeCfg) {
		cfg.actionCode = GenerateActionCode(actions)
	}
}

func WithRateLimitation(subOpt *SubscribeOption) Option {
	return func(cfg *subscribeCfg) {
		cfg.subscribeOpt = subOpt
	}
}

func ConcertGetOption(options *BlockGetOptions) v1.GetOptions {
	if options == nil {
		return v1.GetOptions{}
	}

	return v1.GetOptions{
		TypeMeta:        options.TypeMeta,
		ResourceVersion: options.ResourceVersion,
	}
}

func ConvertGVR(resource *GroupVersionResource) schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    resource.Group,
		Version:  resource.Version,
		Resource: resource.Resource,
	}
}

func NewGroupVersionResource(resource schema.GroupVersionResource) *GroupVersionResource {
	return &GroupVersionResource{
		Group:    resource.Group,
		Version:  resource.Version,
		Resource: resource.Resource,
	}
}

func NewSubscribeResource(grv schema.GroupVersionResource, opts ...Option) SubscribeResource {
	cfg := subscribeCfg{
		actionCode:   1<<SubscribeAction_CREATE | 1<<SubscribeAction_UPDATE | 1<<SubscribeAction_DELETE,
		subscribeOpt: &SubscribeOption{RateLimitation: 0},
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	return SubscribeResource{
		GVR:        NewGroupVersionResource(grv),
		ActionCode: cfg.actionCode,
		Option:     cfg.subscribeOpt,
	}
}

func GenerateActionCode(actionTypes []SubscribeAction) int32 {
	codeSet := make(map[SubscribeAction]struct{}, len(actionTypes))
	for _, action := range actionTypes {
		codeSet[action] = struct{}{}
	}
	return generateActionCode(codeSet)
}

func GenerateSubscribeAction(actionCode int32) []SubscribeAction {
	actionTypes := make([]SubscribeAction, 0)
	if actionCode <= 0 || actionCode >= 1<<(SubscribeAction_DELETE+1) {
		return []SubscribeAction{
			SubscribeAction_DELETE,
			SubscribeAction_UPDATE,
			SubscribeAction_CREATE,
		}
	}

	if actionCode&(1<<SubscribeAction_CREATE) != 0 {
		actionTypes = append(actionTypes, SubscribeAction_CREATE)
	}
	if actionCode&(1<<SubscribeAction_UPDATE) != 0 {
		actionTypes = append(actionTypes, SubscribeAction_UPDATE)
	}
	if actionCode&(1<<SubscribeAction_DELETE) != 0 {
		actionTypes = append(actionTypes, SubscribeAction_DELETE)
	}
	return actionTypes
}

func generateActionCode(actionCode map[SubscribeAction]struct{}) int32 {
	resultCode := 0
	for bit := range actionCode {
		code := 1 << bit
		resultCode |= code
	}
	return int32(resultCode)
}

func NewSubscribeConfirm(subscribe *SubscribeResource) *SubscribeConfirm {
	timestamp := v1.NewTime(time.Now())
	return &SubscribeConfirm{
		Subscribe: subscribe,
		Timestamp: &timestamp,
	}
}

func (m *Error) Error() string {
	return m.Message
}

func PrintGroupVersionResource(gvr *GroupVersionResource) string {
	return fmt.Sprintf("%s.%s.%s", gvr.Group, gvr.Version, gvr.Resource)
}
