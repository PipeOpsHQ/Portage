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
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/kubeexec"
)

func TestRestoreDoesNotSucceedWhenPgIsReadyFails(t *testing.T) {
	t.Parallel()
	r, key := newRestoreHarness(t, &kubeexec.Fake{
		Errors: map[string]error{"ns/pg-0": fmt.Errorf("pg_isready: no response")},
	}, true)
	drain(t, r, key)
	got := getAction(t, r, key)
	if got.Status.Phase == portagev1alpha1.ActionPhaseSucceeded {
		t.Fatalf("Ready pod + failed pg_isready must not Succeeded: %+v", got.Status)
	}
}

func TestRestoreSucceedsAfterPgIsReady(t *testing.T) {
	t.Parallel()
	r, key := newRestoreHarness(t, &kubeexec.Fake{
		Results: map[string]kubeexec.Result{"ns/pg-0": {Stdout: "accepting connections\n"}},
	}, true)
	drain(t, r, key)
	got := getAction(t, r, key)
	if got.Status.Phase != portagev1alpha1.ActionPhaseSucceeded {
		t.Fatalf("phase=%s message=%s", got.Status.Phase, got.Status.Message)
	}
	if got.Status.Attestation == nil {
		t.Fatal("expected attestation")
	}
}

func TestRestoreFailsPreflightWithoutUsefulBackup(t *testing.T) {
	t.Parallel()
	r, key := newRestoreHarness(t, &kubeexec.Fake{}, false)
	drain(t, r, key)
	got := getAction(t, r, key)
	if got.Status.Phase != portagev1alpha1.ActionPhaseFailed {
		t.Fatalf("phase=%s want Failed (%s)", got.Status.Phase, got.Status.Message)
	}
}

func TestBackupFailsWhenLiveSizeIsCertsOnly(t *testing.T) {
	t.Parallel()
	scheme := newScheme(t)
	pol := &portagev1alpha1.Policy{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
		Spec:       portagev1alpha1.PolicySpec{Selector: portagev1alpha1.TargetSelector{Namespaces: []string{"ns"}}},
	}
	act := &portagev1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{Name: "bak", Namespace: "ns"},
		Spec:       portagev1alpha1.ActionSpec{Type: portagev1alpha1.ActionBackup, PolicyRef: "p"},
	}
	c := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&portagev1alpha1.Action{}, &portagev1alpha1.Policy{}).
		WithObjects(pol, act).Build()
	r := &ActionReconciler{
		Client: c,
		Scheme: scheme,
		Kube:   k8sfake.NewSimpleClientset(pgSTS(), pgPod()),
		Exec:   &kubeexec.Fake{Results: map[string]kubeexec.Result{"ns/pg-0": {Stdout: "11800\n"}}},
		Now:    func() time.Time { return time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC) },
	}
	key := types.NamespacedName{Name: "bak", Namespace: "ns"}
	drain(t, r, key)
	got := getAction(t, r, key)
	if got.Status.Phase != portagev1alpha1.ActionPhaseFailed {
		t.Fatalf("phase=%s want Failed (%s)", got.Status.Phase, got.Status.Message)
	}
}

func TestReplicateActionAppliesVolSync(t *testing.T) {
	t.Parallel()
	scheme := newScheme(t)
	pol := &portagev1alpha1.Policy{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
		Spec:       portagev1alpha1.PolicySpec{Selector: portagev1alpha1.TargetSelector{Namespaces: []string{"ns"}}},
	}
	act := &portagev1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{Name: "repl", Namespace: "ns"},
		Spec:       portagev1alpha1.ActionSpec{Type: portagev1alpha1.ActionReplicate, PolicyRef: "p"},
	}
	c := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&portagev1alpha1.Action{}, &portagev1alpha1.Policy{}).
		WithObjects(pol, act).Build()
	r := &ActionReconciler{
		Client: c,
		Scheme: scheme,
		Kube:   k8sfake.NewSimpleClientset(pgSTS(), pgPod()),
		Now:    func() time.Time { return time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC) },
	}
	key := types.NamespacedName{Name: "repl", Namespace: "ns"}
	drain(t, r, key)
	got := getAction(t, r, key)
	if got.Status.Phase == portagev1alpha1.ActionPhaseSucceeded {
		t.Fatalf("replicate must not Succeeded before dest lastSyncTime/basebackup: %s", got.Status.Message)
	}
}

func TestCutoverDoesNotSucceedWithoutProbe(t *testing.T) {
	t.Parallel()
	scheme := newScheme(t)
	pol := &portagev1alpha1.Policy{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
		Spec:       portagev1alpha1.PolicySpec{Selector: portagev1alpha1.TargetSelector{Namespaces: []string{"ns"}}},
	}
	act := &portagev1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{Name: "cut", Namespace: "ns"},
		Spec:       portagev1alpha1.ActionSpec{Type: portagev1alpha1.ActionCutover, PolicyRef: "p"},
		Status:     portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseWaitingReady},
	}
	c := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&portagev1alpha1.Action{}, &portagev1alpha1.Policy{}).
		WithObjects(pol, act).Build()
	r := &ActionReconciler{
		Client: c,
		Scheme: scheme,
		Kube:   k8sfake.NewSimpleClientset(pgSTS(), pgPod()),
		Exec:   &kubeexec.Fake{Errors: map[string]error{"ns/pg-0": fmt.Errorf("pg_isready failed")}},
		Now:    func() time.Time { return time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC) },
	}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "cut", Namespace: "ns"}})
	if err != nil {
		t.Fatal(err)
	}
	got := getAction(t, r, types.NamespacedName{Name: "cut", Namespace: "ns"})
	if got.Status.Phase == portagev1alpha1.ActionPhaseSucceeded {
		t.Fatal("cutover must not succeed without pg_isready")
	}
}

func TestBackupSucceedsWhenLiveSizeIsUseful(t *testing.T) {
	t.Parallel()
	scheme := newScheme(t)
	pol := &portagev1alpha1.Policy{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
		Spec:       portagev1alpha1.PolicySpec{Selector: portagev1alpha1.TargetSelector{Namespaces: []string{"ns"}}},
	}
	act := &portagev1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{Name: "bak", Namespace: "ns"},
		Spec:       portagev1alpha1.ActionSpec{Type: portagev1alpha1.ActionBackup, PolicyRef: "p"},
	}
	c := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&portagev1alpha1.Action{}, &portagev1alpha1.Policy{}).
		WithObjects(pol, act).Build()
	r := &ActionReconciler{
		Client: c,
		Scheme: scheme,
		Kube:   k8sfake.NewSimpleClientset(pgSTS(), pgPod()),
		Exec:   &kubeexec.Fake{Results: map[string]kubeexec.Result{"ns/pg-0": {Stdout: "2097152\n"}}},
		Now:    func() time.Time { return time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC) },
	}
	key := types.NamespacedName{Name: "bak", Namespace: "ns"}
	drain(t, r, key)
	got := getAction(t, r, key)
	if got.Status.Phase != portagev1alpha1.ActionPhaseSucceeded {
		t.Fatalf("phase=%s (%s)", got.Status.Phase, got.Status.Message)
	}
	updated := &portagev1alpha1.Policy{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "p", Namespace: "ns"}, updated); err != nil {
		t.Fatal(err)
	}
	if !updated.Status.BackupHealthy {
		t.Fatalf("expected BackupHealthy: %+v", updated.Status.Artifacts)
	}
}

func newRestoreHarness(t *testing.T, exec kubeexec.Interface, useful bool) (*ActionReconciler, types.NamespacedName) {
	t.Helper()
	scheme := newScheme(t)
	pol := &portagev1alpha1.Policy{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
		Spec:       portagev1alpha1.PolicySpec{Selector: portagev1alpha1.TargetSelector{Namespaces: []string{"ns"}}},
	}
	if useful {
		pol.Status.Artifacts = []portagev1alpha1.ArtifactHealth{{
			Workload:  "ns/StatefulSet/pg",
			Useful:    true,
			SizeBytes: 2 << 20,
			Message:   "logical size floor met",
		}}
	}
	act := &portagev1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{Name: "restore", Namespace: "ns"},
		Spec:       portagev1alpha1.ActionSpec{Type: portagev1alpha1.ActionRestore, PolicyRef: "p"},
	}
	c := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&portagev1alpha1.Action{}, &portagev1alpha1.Policy{}).
		WithObjects(pol, act).Build()
	r := &ActionReconciler{
		Client: c,
		Scheme: scheme,
		Kube:   k8sfake.NewSimpleClientset(pgSTS(), pgPod()),
		Exec:   exec,
		Now:    func() time.Time { return time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC) },
	}
	return r, types.NamespacedName{Name: "restore", Namespace: "ns"}
}

func drain(t *testing.T, r *ActionReconciler, key types.NamespacedName) {
	t.Helper()
	req := ctrl.Request{NamespacedName: key}
	for i := 0; i < 8; i++ {
		res, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
		got := getAction(t, r, key)
		if got.Status.Phase == portagev1alpha1.ActionPhaseSucceeded ||
			got.Status.Phase == portagev1alpha1.ActionPhaseFailed {
			return
		}
		if res.RequeueAfter == 0 && i > 0 {
			return
		}
	}
}

func getAction(t *testing.T, r *ActionReconciler, key types.NamespacedName) *portagev1alpha1.Action {
	t.Helper()
	got := &portagev1alpha1.Action{}
	if err := r.Get(context.Background(), key, got); err != nil {
		t.Fatal(err)
	}
	return got
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := portagev1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func pgSTS() *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "pg", Namespace: "ns", UID: "pg"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: ptr.To(int32(1)),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "postgres", Image: "postgres:16"}},
					Volumes: []corev1.Volume{{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-pg"},
						},
					}},
				},
			},
		},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: 1},
	}
}

func pgPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg-0",
			Namespace: "ns",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "StatefulSet",
				Name:       "pg",
				UID:        "pg",
				Controller: ptr.To(true),
			}},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "postgres", Image: "postgres:16"}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}
}
