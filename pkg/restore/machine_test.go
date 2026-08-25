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

package restore

import (
	"testing"
	"time"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/movers"
)

func pgInv() []classify.Workload {
	return []classify.Workload{{
		Namespace: "ns", Kind: "StatefulSet", Name: "pg",
		Class: portagev1alpha1.ClassSQLLogical, Engine: "postgres",
	}}
}

func TestPreflightFailsWithoutUsefulArtifact(t *testing.T) {
	t.Parallel()
	got := Advance(portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhasePreflight}, Facts{
		Inventory:     pgInv(),
		Useful:        map[string]bool{"ns/StatefulSet/pg": false},
		UsefulMessage: map[string]string{"ns/StatefulSet/pg": "certs-only"},
		Now:           time.Now(),
	})
	if got.Phase != portagev1alpha1.ActionPhaseFailed {
		t.Fatalf("phase=%s want Failed (%s)", got.Phase, got.Message)
	}
}

func TestReadyWithoutPgIsReadyDoesNotSucceed(t *testing.T) {
	t.Parallel()
	got := Advance(portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseWaitingReady}, Facts{
		Inventory:  pgInv(),
		Useful:     map[string]bool{"ns/StatefulSet/pg": true},
		Rehydrated: map[string]bool{"ns/StatefulSet/pg": true},
		Ready:      map[string]bool{"ns/StatefulSet/pg": true},
		Probes: map[string]movers.ProbeResult{
			"ns/StatefulSet/pg": {OK: false, Message: "pg_isready: no response"},
		},
		Now: time.Now(),
	})
	if got.Phase == portagev1alpha1.ActionPhaseSucceeded {
		t.Fatal("Ready without pg_isready must not Succeeded (Velero trap)")
	}
	if got.Phase != portagev1alpha1.ActionPhaseWaitingReady {
		t.Fatalf("phase=%s want WaitingReady", got.Phase)
	}
}

func TestClusterObjectsWithoutDestGetDoesNotSucceed(t *testing.T) {
	t.Parallel()
	key := "ns/ObjectGraph/cluster-objects"
	inv := []classify.Workload{{
		Namespace: "ns", Kind: "ObjectGraph", Name: "cluster-objects",
		Class: portagev1alpha1.ClassClusterObjects,
	}}
	got := Advance(portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseWaitingReady}, Facts{
		Inventory:  inv,
		Useful:     map[string]bool{key: true},
		Rehydrated: map[string]bool{key: true},
		Ready:      map[string]bool{key: true},
		Probes:     map[string]movers.ProbeResult{key: {OK: false, Message: "1/1 dest objects missing"}},
		Now:        time.Now(),
	})
	if got.Phase == portagev1alpha1.ActionPhaseSucceeded {
		t.Fatal("object apply without dest Get must not Succeeded")
	}
}

func TestAttestRefusesSucceededWithoutProbe(t *testing.T) {
	t.Parallel()
	got := Advance(portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseAttesting}, Facts{
		Inventory: pgInv(),
		Ready:     map[string]bool{"ns/StatefulSet/pg": true},
		Probes:    map[string]movers.ProbeResult{"ns/StatefulSet/pg": {OK: false, Message: "pg_isready failed"}},
		Now:       time.Now(),
	})
	if got.Phase == portagev1alpha1.ActionPhaseSucceeded {
		t.Fatal("Attesting must not skip CanSucceed")
	}
}

func TestSucceedsAfterPgIsReady(t *testing.T) {
	t.Parallel()
	got := Advance(portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhaseWaitingReady}, Facts{
		Inventory:  pgInv(),
		Useful:     map[string]bool{"ns/StatefulSet/pg": true},
		Rehydrated: map[string]bool{"ns/StatefulSet/pg": true},
		Ready:      map[string]bool{"ns/StatefulSet/pg": true},
		Probes: map[string]movers.ProbeResult{
			"ns/StatefulSet/pg": {OK: true, Message: "pg_isready"},
		},
		Now: time.Now(),
	})
	if got.Phase != portagev1alpha1.ActionPhaseSucceeded {
		t.Fatalf("phase=%s want Succeeded (%s)", got.Phase, got.Message)
	}
	if got.Attestation == nil || !got.Terminal {
		t.Fatal("expected attestation and terminal")
	}
}

func TestDryRunDoesNotRehydrate(t *testing.T) {
	t.Parallel()
	got := Advance(portagev1alpha1.ActionStatus{Phase: portagev1alpha1.ActionPhasePreflight}, Facts{
		Inventory: pgInv(),
		Useful:    map[string]bool{"ns/StatefulSet/pg": true},
		DryRun:    true,
		Now:       time.Now(),
	})
	if got.Phase != portagev1alpha1.ActionPhaseSucceeded || got.Attestation == nil || got.Attestation.Source != "dry-run" {
		t.Fatalf("dry-run: %+v", got)
	}
}
