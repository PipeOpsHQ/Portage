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

package workloads

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	"github.com/PipeOpsHQ/portage/pkg/classify"
)

func TestScaleRemembersOriginal(t *testing.T) {
	t.Parallel()
	w := classify.Workload{Namespace: "ns", Name: "pg", Kind: "StatefulSet"}
	kube := k8sfake.NewSimpleClientset(&appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "pg", Namespace: "ns"},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptr.To(int32(3))},
	})
	if err := Scale(context.Background(), kube, w, 0); err != nil {
		t.Fatal(err)
	}
	sts, err := kube.AppsV1().StatefulSets("ns").Get(context.Background(), "pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 0 {
		t.Fatal("expected freeze to 0")
	}
	if OriginalReplicas(sts.Annotations) != 3 {
		t.Fatalf("original=%s", sts.Annotations[originalReplicasAnn])
	}
}
