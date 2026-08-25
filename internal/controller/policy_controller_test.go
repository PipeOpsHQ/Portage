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

package controller

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
)

func TestAutoRestoreCreatesActionWhenPVCMissing(t *testing.T) {
	t.Parallel()
	scheme := newScheme(t)
	pol := &portagev1alpha1.Policy{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
		Spec: portagev1alpha1.PolicySpec{
			Selector: portagev1alpha1.TargetSelector{Namespaces: []string{"ns"}},
			Restore:  portagev1alpha1.RestoreSpec{Auto: true},
		},
		Status: portagev1alpha1.PolicyStatus{
			BackupHealthy: true,
			Artifacts: []portagev1alpha1.ArtifactHealth{{
				Workload: "ns/StatefulSet/pg", Useful: true, SizeBytes: 2 << 20,
			}},
		},
	}
	c := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&portagev1alpha1.Policy{}, &portagev1alpha1.Action{}).
		WithObjects(pol).Build()
	r := &PolicyReconciler{
		Client:     c,
		Scheme:     scheme,
		KubeClient: k8sfake.NewSimpleClientset(pgSTS(), pgPod()), // STS references data-pg, PVC absent
	}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "p", Namespace: "ns"}})
	if err != nil {
		t.Fatal(err)
	}
	act := &portagev1alpha1.Action{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "restore-auto-p", Namespace: "ns"}, act); err != nil {
		t.Fatalf("expected auto restore Action: %v", err)
	}
	if act.Spec.Type != portagev1alpha1.ActionRestore {
		t.Fatalf("type=%s", act.Spec.Type)
	}
}

func TestAutoRestoreSkippedWhenDisabled(t *testing.T) {
	t.Parallel()
	scheme := newScheme(t)
	pol := &portagev1alpha1.Policy{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
		Spec:       portagev1alpha1.PolicySpec{Selector: portagev1alpha1.TargetSelector{Namespaces: []string{"ns"}}},
	}
	c := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&portagev1alpha1.Policy{}).
		WithObjects(pol).Build()
	r := &PolicyReconciler{Client: c, Scheme: scheme, KubeClient: k8sfake.NewSimpleClientset(pgSTS())}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "p", Namespace: "ns"}})
	if err != nil {
		t.Fatal(err)
	}
	act := &portagev1alpha1.Action{}
	err = c.Get(context.Background(), types.NamespacedName{Name: "restore-auto-p", Namespace: "ns"}, act)
	if err == nil {
		t.Fatal("must not auto-restore when auto=false")
	}
}
