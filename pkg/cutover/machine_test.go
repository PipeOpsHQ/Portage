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
