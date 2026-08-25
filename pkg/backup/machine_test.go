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

package backup

import (
	"testing"
	"time"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
)

func TestBackupActionFailsOnCertsOnly(t *testing.T) {
	t.Parallel()
	inv := []classify.Workload{{
		Namespace: "ns", Kind: "StatefulSet", Name: "pg",
		Class: portagev1alpha1.ClassSQLLogical, Engine: "postgres",
	}}
	got := Advance(portagev1alpha1.ActionStatus{}, Facts{
		Inventory: inv,
		Artifacts: []portagev1alpha1.ArtifactHealth{{
			Workload:  "ns/StatefulSet/pg",
			Useful:    false,
			SizeBytes: 11800,
			Message:   "certs-only",
		}},
		Now: time.Now(),
	})
	if got.Phase != portagev1alpha1.ActionPhaseFailed {
		t.Fatalf("phase=%s want Failed", got.Phase)
	}
}

func TestBackupActionSucceedsWhenUseful(t *testing.T) {
	t.Parallel()
	inv := []classify.Workload{{
		Namespace: "ns", Kind: "StatefulSet", Name: "pg",
		Class: portagev1alpha1.ClassSQLLogical, Engine: "postgres",
	}}
	got := Advance(portagev1alpha1.ActionStatus{}, Facts{
		Inventory: inv,
		Artifacts: []portagev1alpha1.ArtifactHealth{{
			Workload:  "ns/StatefulSet/pg",
			Useful:    true,
			SizeBytes: 3 << 20,
		}},
		Now: time.Now(),
	})
	if got.Phase != portagev1alpha1.ActionPhaseSucceeded {
		t.Fatalf("phase=%s (%s)", got.Phase, got.Message)
	}
}
