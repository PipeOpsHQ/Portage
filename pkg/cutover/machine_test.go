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

package cutover

import (
	"strings"
	"testing"
	"time"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/movers"
)

func pg() []classify.Workload {
	return []classify.Workload{{Namespace: "ns", Kind: "StatefulSet", Name: "pg", Class: portagev1alpha1.ClassSQLLogical, Engine: "postgres"}}
}

func TestCutoverWaitsForLagThenProbe(t *testing.T) {
	t.Parallel()
	st := portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseCatchingUp}
	got := Advance(st, Facts{Inventory: pg(), Frozen: true, LagZero: false, Now: time.Now()})
	if got.Phase != portagev1alpha1.ActionPhaseCatchingUp {
		t.Fatalf("phase=%s", got.Phase)
	}
	got = Advance(st, Facts{Inventory: pg(), Frozen: true, LagZero: true, Now: time.Now()})
	if got.Phase != portagev1alpha1.ActionPhasePromoting {
		t.Fatalf("phase=%s want Promoting", got.Phase)
	}
}

func TestCutoverDoesNotSucceedWithoutPgIsReady(t *testing.T) {
	t.Parallel()
	got := Advance(portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseWaitingReady}, Facts{
		Inventory: pg(),
		Frozen:    true, LagZero: true, Promoted: true, Switched: true,
		Ready:  map[string]bool{"ns/StatefulSet/pg": true},
		Probes: map[string]movers.ProbeResult{"ns/StatefulSet/pg": {OK: false, Message: "pg_isready failed"}},
		Now:    time.Now(),
	})
	if got.Phase == portagev1alpha1.ActionPhaseSucceeded {
		t.Fatal("cutover must not Succeeded without probe")
	}
}

func TestRollbackFromSwitching(t *testing.T) {
	t.Parallel()
	got := Advance(portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseSwitching}, Facts{
		Inventory: pg(), Rollback: true, Now: time.Now(),
	})
	if got.Phase != portagev1alpha1.ActionPhaseRolledBack {
		t.Fatalf("phase=%s", got.Phase)
	}
}

func TestCutoverSucceedsAfterSwitchAndProbe(t *testing.T) {
	t.Parallel()
	got := Advance(portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseWaitingReady}, Facts{
		Inventory: pg(),
		Frozen:    true, LagZero: true, Promoted: true, Switched: true,
		Ready:  map[string]bool{"ns/StatefulSet/pg": true},
		Probes: map[string]movers.ProbeResult{"ns/StatefulSet/pg": {OK: true, Message: "pg_isready"}},
		Now:    time.Now(),
	})
	if got.Phase != portagev1alpha1.ActionPhaseSucceeded {
		t.Fatalf("phase=%s %s", got.Phase, got.Message)
	}
	if got.Attestation == nil || got.Attestation.Source != "cutover" {
		t.Fatal("expected cutover attestation")
	}
}

func mixed() []classify.Workload {
	return append(pg(), classify.Workload{Namespace: "ns", Kind: "Deployment", Name: "web", Class: portagev1alpha1.ClassStateless})
}

func TestCutoverMixedPolicySucceedsWhenStatelessReady(t *testing.T) {
	t.Parallel()
	got := Advance(portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseWaitingReady}, Facts{
		Inventory: mixed(),
		Ready:     map[string]bool{"ns/StatefulSet/pg": true, "ns/Deployment/web": true},
		Probes:    map[string]movers.ProbeResult{"ns/StatefulSet/pg": {OK: true, Message: "pg_isready"}},
		Now:       time.Now(),
	})
	if got.Phase != portagev1alpha1.ActionPhaseSucceeded {
		t.Fatalf("phase=%s %s", got.Phase, got.Message)
	}
}

func TestCutoverWaitingReadyTimesOut(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 4, 22, 0, 0, 0, time.UTC)
	facts := Facts{
		Inventory:  mixed(),
		Ready:      map[string]bool{"ns/StatefulSet/pg": true},
		Notes:      map[string]string{"ns/Deployment/web": "dest not Ready"},
		Probes:     map[string]movers.ProbeResult{"ns/StatefulSet/pg": {OK: true}},
		SwitchedAt: now.Add(-10 * time.Minute),
		Now:        now,
	}
	st := portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseWaitingReady}
	if got := Advance(st, facts); got.Phase != portagev1alpha1.ActionPhaseWaitingReady || !strings.Contains(got.Message, "web is not Ready on dest: dest not Ready") {
		t.Fatalf("before timeout: %s %s", got.Phase, got.Message)
	}
	facts.SwitchedAt = now.Add(-DefaultReadyTimeout - time.Second)
	got := Advance(st, facts)
	if got.Phase != portagev1alpha1.ActionPhaseFailed || !got.Terminal || !strings.Contains(got.Message, "spec.rollback") {
		t.Fatalf("after timeout: %s %s", got.Phase, got.Message)
	}
	if UnfreezeOnFail(portagev1alpha1.ActionPhaseWaitingReady) {
		t.Fatal("traffic is on dest; a WaitingReady failure must not unfreeze source")
	}
}

func TestCutoverAbortsWhenLagBlocked(t *testing.T) {
	t.Parallel()
	got := Advance(portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseCatchingUp}, Facts{
		Inventory: pg(), LagBlocked: true, LagReason: "ReplicationDestination paused", Now: time.Now(),
	})
	if got.Phase != portagev1alpha1.ActionPhaseFailed || !strings.Contains(got.Message, "paused") {
		t.Fatalf("phase=%s %s", got.Phase, got.Message)
	}
	if !UnfreezeOnFail(portagev1alpha1.ActionPhaseCatchingUp) {
		t.Fatal("abort before switch must unfreeze source")
	}
}

func TestCutoverFreezeTimeout(t *testing.T) {
	t.Parallel()
	now := time.Now()
	st := portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseCatchingUp}
	facts := Facts{Inventory: pg(), LagReason: "dest trails source", FrozenAt: now.Add(-4 * time.Minute), FreezeTimeout: 5 * time.Minute, Now: now}
	if got := Advance(st, facts); got.Phase != portagev1alpha1.ActionPhaseCatchingUp || !strings.Contains(got.Message, "dest trails source") {
		t.Fatalf("%s %s", got.Phase, got.Message)
	}
	facts.FrozenAt = now.Add(-6 * time.Minute)
	if got := Advance(st, facts); got.Phase != portagev1alpha1.ActionPhaseFailed || !strings.Contains(got.Message, "freeze exceeded 5m0s") {
		t.Fatalf("%s %s", got.Phase, got.Message)
	}
}

func TestCutoverPreflightErrorFailsBeforeFreeze(t *testing.T) {
	t.Parallel()
	got := Advance(portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhasePending}, Facts{
		Inventory: pg(), PreflightError: "fsGroup", Now: time.Now(),
	})
	if got.Phase != portagev1alpha1.ActionPhaseFailed || !strings.HasPrefix(got.Message, "preflight:") {
		t.Fatalf("%s %s", got.Phase, got.Message)
	}
}

func TestRollbackFromWaitingReadyAndSucceeded(t *testing.T) {
	t.Parallel()
	for _, p := range []portagev1alpha1.ActionPhase{portagev1alpha1.ActionPhaseWaitingReady, portagev1alpha1.ActionPhaseSucceeded, portagev1alpha1.ActionPhaseFailed} {
		got := Advance(portagev1alpha1.ActionStatus{Phase: p}, Facts{Inventory: pg(), Rollback: true, Now: time.Now()})
		if got.Phase != portagev1alpha1.ActionPhaseRolledBack {
			t.Fatalf("%s: phase=%s", p, got.Phase)
		}
	}
}
