package kubeproxy

import (
	"fmt"
	"testing"

	"github.com/asynkron/protoactor-go/remote"
)

func TestNewSubscribeResource(t *testing.T) {
	res := &SubscribeResourceFor{Resource: &SubscribeResource{
		GVR:        nil,
		ActionCode: 0,
		Option:     nil,
	}}
	buffer, typeName, err := remote.Serialize(res, 0)
	if err != nil {
		fmt.Printf("Serialize with error due to %v \n", err)
		return
	}
	fmt.Printf("buffer size %d, type %s \n", len(buffer), typeName)
}
