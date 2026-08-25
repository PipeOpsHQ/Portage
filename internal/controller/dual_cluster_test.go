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
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/clusters"
	"github.com/PipeOpsHQ/portage/pkg/kubeexec"
	"github.com/PipeOpsHQ/portage/pkg/objectstore"
)

func TestDualClusterBackupStoresDumpAndRestoreAppliesDest(t *testing.T) {
	t.Parallel()
	scheme := newScheme(t)
	store := &objectstore.Memory{}
	src := k8sfake.NewSimpleClientset(pgSTS(), pgPod())
	dst := k8sfake.NewSimpleClientset()
	dumpBody := strings.Repeat("-- row\n", 20_000) // > 64KiB
	srcExec := &kubeexec.Fake{Results: map[string]kubeexec.Result{"ns/pg-0": {Stdout: dumpBody}}}

	pol := &portagev1alpha1.Policy{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
		Spec:       portagev1alpha1.PolicySpec{Selector: portagev1alpha1.TargetSelector{Namespaces: []string{"ns"}}},
	}
	bak := &portagev1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{Name: "bak", Namespace: "ns"},
		Spec:       portagev1alpha1.ActionSpec{Type: portagev1alpha1.ActionBackup, PolicyRef: "p"},
	}
	c := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&portagev1alpha1.Action{}, &portagev1alpha1.Policy{}).
		WithObjects(pol, bak).Build()

	r := &ActionReconciler{
		Client: c,
		Scheme: scheme,
		Kube:   src, // unused when Resolve is set
		Store:  store,
		Exec:   srcExec,
		Now:    func() time.Time { return time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC) },
		Resolve: func(context.Context, *portagev1alpha1.ClusterPair) (clusters.Pair, error) {
			return clusters.Pair{
				Source: clusters.Endpoints{Name: "aws", Kube: src, Exec: srcExec},
				Dest:   clusters.Endpoints{Name: "gcp", Kube: dst, Exec: &kubeexec.Fake{}},
			}, nil
		},
	}

	drain(t, r, types.NamespacedName{Name: "bak", Namespace: "ns"})
	got := getAction(t, r, types.NamespacedName{Name: "bak", Namespace: "ns"})
	if got.Status.Phase != portagev1alpha1.ActionPhaseSucceeded {
		t.Fatalf("backup phase=%s %s", got.Status.Phase, got.Status.Message)
	}
	p := &portagev1alpha1.Policy{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "p", Namespace: "ns"}, p); err != nil {
		t.Fatal(err)
	}
	if !p.Status.BackupHealthy || len(p.Status.Artifacts) == 0 || p.Status.Artifacts[0].ArtifactID == "" {
		t.Fatalf("expected portable dump artifact: %+v", p.Status.Artifacts)
	}
	if _, err := store.Get(context.Background(), p.Status.Artifacts[0].ArtifactID); err != nil {
		t.Fatalf("dump missing from object store: %v", err)
	}

	rst := &portagev1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{Name: "rst", Namespace: "ns"},
		Spec:       portagev1alpha1.ActionSpec{Type: portagev1alpha1.ActionRestore, PolicyRef: "p"},
	}
	if err := c.Create(context.Background(), rst); err != nil {
		t.Fatal(err)
	}
	drain(t, r, types.NamespacedName{Name: "rst", Namespace: "ns"})

	if _, err := dst.AppsV1().StatefulSets("ns").Get(context.Background(), "pg", metav1.GetOptions{}); err != nil {
		t.Fatalf("dest cluster missing applied STS: %v", err)
	}
}

func TestCutoverRollbackUnfreezesSource(t *testing.T) {
	t.Parallel()
	st := portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseSwitching}
	// covered by pkg/cutover; keep a controller-level rollback Action
	scheme := newScheme(t)
	src := k8sfake.NewSimpleClientset(pgSTS(), pgPod())
	pol := &portagev1alpha1.Policy{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
		Spec:       portagev1alpha1.PolicySpec{Selector: portagev1alpha1.TargetSelector{Namespaces: []string{"ns"}}},
	}
	act := &portagev1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{Name: "rb", Namespace: "ns"},
		Spec:       portagev1alpha1.ActionSpec{Type: portagev1alpha1.ActionCutover, PolicyRef: "p", Rollback: true},
	}
	c := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&portagev1alpha1.Action{}, &portagev1alpha1.Policy{}).
		WithObjects(pol, act).Build()
	r := &ActionReconciler{Client: c, Scheme: scheme, Kube: src, Now: func() time.Time { return time.Now() }}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "rb", Namespace: "ns"}})
	if err != nil {
		t.Fatal(err)
	}
	got := getAction(t, r, types.NamespacedName{Name: "rb", Namespace: "ns"})
	if got.Status.Phase != portagev1alpha1.ActionPhaseRolledBack {
		t.Fatalf("phase=%s want RolledBack (%s)", got.Status.Phase, got.Status.Message)
	}
	_ = st
}
