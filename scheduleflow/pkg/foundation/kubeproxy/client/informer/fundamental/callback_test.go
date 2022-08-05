package fundamental

import (
	"testing"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/scheduleflow/pkg/apis/kubeproxy"
	"github.com/smartystreets/goconvey/convey"
)

func TestFormKey(t *testing.T) {
	pid := actor.NewPID("localhost:8080", "test")
	gvr := &kubeproxy.GroupVersionResource{
		Group:    "subscribeGroup",
		Version:  "v1",
		Resource: "resource",
	}

	convey.Convey("test Key", t, func() {
		convey.Convey("test Form PID-GVR key string", func() {
			key, err := FormPIDGVRKeyString(pid, gvr)
			convey.So(err, convey.ShouldBeNil)
			newKeyStruct, err := FormPIDGVRKeyStruct(key)
			convey.So(err, convey.ShouldBeNil)
			convey.So(newKeyStruct, convey.ShouldResemble, &PIDGVRStruct{PID: pid, GroupVersionResource: gvr})
		})

		actionType := kubeproxy.SubscribeAction(1)
		convey.Convey("TestFormEvenKeyStruct", func() {
			key, err := FormEventKeyString(pid, gvr, actionType)
			convey.So(err, convey.ShouldBeNil)
			keyStruct, err := FormEvenKeyStruct(key)
			convey.So(err, convey.ShouldBeNil)
			convey.So(keyStruct, convey.ShouldResemble, &EventKeyStruct{PIDGVRStruct{PID: pid, GroupVersionResource: gvr}, &actionType})
		})
	})
}
