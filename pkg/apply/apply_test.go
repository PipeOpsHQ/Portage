/*
Copyright 2026 PipeOps and the Portage Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package apply

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

func TestTypedAppliesSTSToDest(t *testing.T) {
	t.Parallel()
	kube := k8sfake.NewSimpleClientset()
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "pg", Namespace: "ns"}}
	m, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(sts)
	u := &unstructured.Unstructured{Object: m}
	u.SetKind("StatefulSet")
	u.SetAPIVersion("apps/v1")
	if err := Typed(context.Background(), kube, []*unstructured.Unstructured{u}); err != nil {
		t.Fatal(err)
	}
	got, err := kube.AppsV1().StatefulSets("ns").Get(context.Background(), "pg", metav1.GetOptions{})
	if err != nil || got.Name != "pg" {
		t.Fatalf("dest missing STS: %v", err)
	}
}
