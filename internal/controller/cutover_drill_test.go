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

// Regression tests for the v0.2.16 cutover drill findings.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/kubeexec"
)

type cutoverHarness struct {
	r    *ActionReconciler
	kube *k8sfake.Clientset
	key  types.NamespacedName
}

func newCutoverHarness(t *testing.T, st portagev1alpha1.ActionStatus, spec portagev1alpha1.ActionSpec, hookURL string, objs ...*appsv1.StatefulSet) cutoverHarness {
	t.Helper()
	scheme := newScheme(t)
	pol := &portagev1alpha1.Policy{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
		Spec: portagev1alpha1.PolicySpec{
			Selector: portagev1alpha1.TargetSelector{Namespaces: []string{"ns"}},
			Cutover:  portagev1alpha1.CutoverSpec{TrafficHook: hookURL},
		},
	}
	spec.Type, spec.PolicyRef = portagev1alpha1.ActionCutover, "p"
	act := &portagev1alpha1.Action{ObjectMeta: metav1.ObjectMeta{Name: "cut", Namespace: "ns"}, Spec: spec, Status: st}
	c := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&portagev1alpha1.Action{}, &portagev1alpha1.Policy{}).
		WithObjects(pol, act).Build()
	sts := pgSTS()
	if len(objs) > 0 {
		sts = objs[0]
	}
	kube := k8sfake.NewSimpleClientset(sts, pgPod(), statelessDep("web", 1))
	r := &ActionReconciler{Client: c, Scheme: scheme, Kube: kube, Exec: &kubeexec.Fake{Results: map[string]kubeexec.Result{"ns/pg-0": {Stdout: "accepting connections\n"}}}, Now: time.Now}
	return cutoverHarness{r: r, kube: kube, key: types.NamespacedName{Name: "cut", Namespace: "ns"}}
}

func (h cutoverHarness) reconcile(t *testing.T) *portagev1alpha1.Action {
	t.Helper()
	if _, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: h.key}); err != nil {
		t.Fatal(err)
	}
	return getAction(t, h.r, h.key)
}

func (h cutoverHarness) replicas(t *testing.T) int32 {
	t.Helper()
	sts, err := h.kube.AppsV1().StatefulSets("ns").Get(context.Background(), "pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return *sts.Spec.Replicas
}

func frozenPG(replicas int32) *appsv1.StatefulSet {
	sts := pgSTS()
	sts.Spec.Replicas = ptr.To(int32(0))
	sts.Annotations = map[string]string{"portage.io/original-replicas": strconv.Itoa(int(replicas))}
	return sts
}

// Bug 2: a Stateless Deployment that is Ready on dest no longer stalls WaitingReady.
func TestCutoverMixedPolicyReachesSucceeded(t *testing.T) {
	t.Parallel()
	h := newCutoverHarness(t, portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseWaitingReady}, portagev1alpha1.ActionSpec{}, "")
	got := h.reconcile(t)
	if got.Status.Phase != portagev1alpha1.ActionPhaseSucceeded {
		t.Fatalf("phase=%s %s %+v", got.Status.Phase, got.Status.Message, got.Status.Workloads)
	}
	if !statelessReady(got) {
		t.Fatalf("stateless workload not reported Ready: %+v", got.Status.Workloads)
	}
}

// Bug 3: spec.rollback patched onto a running Action takes effect on the next pass.
func TestCutoverRollbackPatchedMidFlight(t *testing.T) {
	t.Parallel()
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.URL.Path)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	h := newCutoverHarness(t, portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseWaitingReady}, portagev1alpha1.ActionSpec{}, srv.URL, frozenPG(3))
	// Hold WaitingReady: dest Deployment not Ready.
	dep, _ := h.kube.AppsV1().Deployments("ns").Get(context.Background(), "web", metav1.GetOptions{})
	dep.Status.ReadyReplicas = 0
	_, _ = h.kube.AppsV1().Deployments("ns").UpdateStatus(context.Background(), dep, metav1.UpdateOptions{})
	if got := h.reconcile(t); got.Status.Phase != portagev1alpha1.ActionPhaseWaitingReady {
		t.Fatalf("setup: phase=%s %s", got.Status.Phase, got.Status.Message)
	}

	act := getAction(t, h.r, h.key)
	act.Spec.Rollback = true
	if err := h.r.Update(context.Background(), act); err != nil {
		t.Fatal(err)
	}
	got := h.reconcile(t)
	if got.Status.Phase != portagev1alpha1.ActionPhaseRolledBack {
		t.Fatalf("phase=%s %s", got.Status.Phase, got.Status.Message)
	}
	if n := h.replicas(t); n != 3 {
		t.Fatalf("source replicas=%d want 3 (original)", n)
	}
	if len(calls) == 0 {
		t.Fatal("rollback hook not called")
	}
}

// Documented failback after a successful cutover used to be dropped at the
// top of Reconcile.
func TestCutoverRollbackAfterSucceeded(t *testing.T) {
	t.Parallel()
	h := newCutoverHarness(t, portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseSucceeded, CompletionTime: &metav1.Time{Time: time.Now()}},
		portagev1alpha1.ActionSpec{Rollback: true}, "", frozenPG(1))
	got := h.reconcile(t)
	if got.Status.Phase != portagev1alpha1.ActionPhaseRolledBack {
		t.Fatalf("phase=%s %s", got.Status.Phase, got.Status.Message)
	}
	if n := h.replicas(t); n != 1 {
		t.Fatalf("source replicas=%d want 1", n)
	}
}

// After traffic moved, a failing rollback hook must not unfreeze source
// (two writable primaries). It retries instead.
func TestCutoverRollbackKeepsSourceFrozenWhenHookFails(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)
	h := newCutoverHarness(t, portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseWaitingReady},
		portagev1alpha1.ActionSpec{Rollback: true}, srv.URL, frozenPG(1))
	got := h.reconcile(t)
	if got.Status.Phase != portagev1alpha1.ActionPhaseWaitingReady || !strings.Contains(got.Status.Message, "rollback pending") {
		t.Fatalf("phase=%s %s", got.Status.Phase, got.Status.Message)
	}
	if n := h.replicas(t); n != 0 {
		t.Fatalf("source replicas=%d; must stay frozen", n)
	}
}

// Rolling back an Action that never froze must not touch source replicas.
func TestCutoverRollbackLeavesUnfrozenSourceAlone(t *testing.T) {
	t.Parallel()
	sts := pgSTS()
	sts.Spec.Replicas = ptr.To(int32(3))
	h := newCutoverHarness(t, portagev1alpha1.ActionStatus{}, portagev1alpha1.ActionSpec{Rollback: true}, "", sts)
	got := h.reconcile(t)
	if got.Status.Phase != portagev1alpha1.ActionPhaseRolledBack {
		t.Fatalf("phase=%s %s", got.Status.Phase, got.Status.Message)
	}
	if n := h.replicas(t); n != 3 {
		t.Fatalf("source replicas=%d want 3", n)
	}
}

// Bug 4: a source Postgres with fsGroup is refused before the freeze.
func TestCutoverPreflightRefusesPostgresFSGroup(t *testing.T) {
	t.Parallel()
	sts := pgSTS()
	sts.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{FSGroup: ptr.To(int64(0))}
	h := newCutoverHarness(t, portagev1alpha1.ActionStatus{}, portagev1alpha1.ActionSpec{}, "", sts)
	got := h.reconcile(t)
	if got.Status.Phase != portagev1alpha1.ActionPhaseFailed || !strings.Contains(got.Status.Message, "fsGroup") {
		t.Fatalf("phase=%s %s", got.Status.Phase, got.Status.Message)
	}
	if n := h.replicas(t); n != 1 {
		t.Fatalf("source replicas=%d; preflight must not freeze", n)
	}
}

// Marketplace Postgres keeps fsGroup (tenant isolation) with
// OnRootMismatch. That is allowed when the restart is verified safe.
func marketplacePG() *appsv1.StatefulSet {
	sts := pgSTS()
	onRoot := corev1.FSGroupChangeOnRootMismatch
	sts.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{FSGroup: ptr.To(int64(0)), FSGroupChangePolicy: &onRoot}
	return sts
}

func TestCutoverPreflightAllowsVerifiedFSGroup(t *testing.T) {
	t.Parallel()
	h := newCutoverHarness(t, portagev1alpha1.ActionStatus{}, portagev1alpha1.ActionSpec{}, "", marketplacePG())
	h.r.Exec = &kubeexec.Fake{Results: map[string]kubeexec.Result{"ns/pg-0": {Stdout: "key 600 999 /var/lib/postgresql/data/pgdata/server.key\n"}}}
	got := h.reconcile(t)
	if got.Status.Phase != portagev1alpha1.ActionPhaseQuiescing {
		t.Fatalf("phase=%s %s", got.Status.Phase, got.Status.Message)
	}
	if n := h.replicas(t); n != 0 {
		t.Fatalf("source replicas=%d; verified source should freeze", n)
	}
}

func TestCutoverPreflightRefusesRewrittenKey(t *testing.T) {
	t.Parallel()
	h := newCutoverHarness(t, portagev1alpha1.ActionStatus{}, portagev1alpha1.ActionSpec{}, "", marketplacePG())
	h.r.Exec = &kubeexec.Fake{Results: map[string]kubeexec.Result{"ns/pg-0": {Stdout: "key 660 999 /var/lib/postgresql/data/pgdata/server.key\n"}}}
	got := h.reconcile(t)
	if got.Status.Phase != portagev1alpha1.ActionPhaseFailed || !strings.Contains(got.Status.Message, "chmod 600") {
		t.Fatalf("phase=%s %s", got.Status.Phase, got.Status.Message)
	}
	if n := h.replicas(t); n != 1 {
		t.Fatalf("source replicas=%d; preflight must not freeze", n)
	}
}

func TestCutoverDryRunDoesNotFreezeSource(t *testing.T) {
	t.Parallel()
	h := newCutoverHarness(t, portagev1alpha1.ActionStatus{}, portagev1alpha1.ActionSpec{DryRun: true}, "")
	got := h.reconcile(t)
	if got.Status.Phase != portagev1alpha1.ActionPhaseSucceeded {
		t.Fatalf("phase=%s %s", got.Status.Phase, got.Status.Message)
	}
	if n := h.replicas(t); n != 1 {
		t.Fatalf("source replicas=%d; dry-run must not freeze", n)
	}
}

// Bug 1 + freeze bound: lag that never reaches zero aborts and unfreezes.
func TestCutoverCatchingUpAbortsAfterFreezeTimeout(t *testing.T) {
	t.Parallel()
	st := portagev1alpha1.ActionStatus{
		Phase:      portagev1alpha1.ActionPhaseCatchingUp,
		Conditions: []metav1.Condition{{Type: "SourceFrozen", Status: metav1.ConditionTrue, Reason: "ScaledToZero", LastTransitionTime: metav1.NewTime(time.Now().Add(-2 * time.Minute))}},
	}
	h := newCutoverHarness(t, st, portagev1alpha1.ActionSpec{FreezeTimeout: "1m"}, "", frozenPG(1))
	got := h.reconcile(t)
	if got.Status.Phase != portagev1alpha1.ActionPhaseFailed || !strings.Contains(got.Status.Message, "freeze exceeded") {
		t.Fatalf("phase=%s %s", got.Status.Phase, got.Status.Message)
	}
	if n := h.replicas(t); n != 1 {
		t.Fatalf("source replicas=%d want 1 after abort", n)
	}
}
