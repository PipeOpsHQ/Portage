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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
)

// Facts are observations for a Backup Action.
type Facts struct {
	Inventory []classify.Workload
	Artifacts []portagev1alpha1.ArtifactHealth
	Now       time.Time
}

// Result is the next Backup Action status.
type Result struct {
	Phase        portagev1alpha1.ActionPhase
	Message      string
	Workloads    []portagev1alpha1.WorkloadActionStatus
	Attestation  *portagev1alpha1.Attestation
	RequeueAfter time.Duration
	Terminal     bool
}

// Advance moves a Backup Action. Succeeded requires every stateful artifact
// Useful — a Completed snapshot job is not enough.
func Advance(status portagev1alpha1.ActionStatus, facts Facts) Result {
	workloads := workloadsFrom(facts.Inventory, facts.Artifacts)
	ok, missing := Healthy(classify.Inventory{Workloads: facts.Inventory}, facts.Artifacts)
	if !ok {
		msg := "backup not useful"
		if len(missing) > 0 {
			msg = "backup not useful: " + missing[0]
		}
		return Result{
			Phase:     portagev1alpha1.ActionPhaseFailed,
			Message:   msg,
			Workloads: workloads,
			Terminal:  true,
		}
	}
	return Result{
		Phase:     portagev1alpha1.ActionPhaseSucceeded,
		Message:   "all stateful artifacts useful",
		Workloads: workloads,
		Terminal:  true,
		Attestation: &portagev1alpha1.Attestation{
			RecordedAt: metav1.NewTime(facts.Now),
			Source:     "controller",
			Workloads:  workloads,
		},
	}
}

func workloadsFrom(inv []classify.Workload, arts []portagev1alpha1.ArtifactHealth) []portagev1alpha1.WorkloadActionStatus {
	byKey := map[string]portagev1alpha1.ArtifactHealth{}
	for _, a := range arts {
		byKey[a.Workload] = a
	}
	out := make([]portagev1alpha1.WorkloadActionStatus, 0, len(inv))
	for _, w := range inv {
		st := portagev1alpha1.WorkloadActionStatus{
			Name:   w.Name,
			Key:    w.Key(),
			Class:  w.Class,
			Engine: w.Engine,
			Ready:  true, // backup does not start dest workloads
		}
		if a, ok := byKey[w.Key()]; ok {
			st.ProbeOK = a.Useful
			st.Probe = a.Message
			st.Message = a.Message
		}
		if w.Class == portagev1alpha1.ClassStateless {
			st.ProbeOK = true
			st.Probe = "stateless"
		}
		out = append(out, st)
	}
	return out
}
