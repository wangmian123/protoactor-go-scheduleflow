package dispacher

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestDispatcher_Process(t *testing.T) {
	node := v1.Node{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
		},
		Spec:   v1.NodeSpec{},
		Status: v1.NodeStatus{},
	}

	buffer, _ := json.Marshal(node)

	fmt.Println(string(buffer))
	obj := unstructured.Unstructured{}
	_ = json.Unmarshal(buffer, &obj)
	fmt.Println(obj)

	fmt.Println(getNestedString(obj.Object, "metadata", "name"))
}
