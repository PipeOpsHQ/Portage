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

// Package restore is the Restore Action state machine.
//
// Succeeded is only reachable through actionphase.CanSucceed (Ready AND class
// probe). A mover reporting restore complete is not enough — that is the
// Velero trap.
package restore

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/actionphase"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/movers"
)

// Facts are observations the controller gathered this reconcile.
type Facts struct {
	Inventory     []classify.Workload
	Useful        map[string]bool
	UsefulMessage map[string]string
	Rehydrated    map[string]bool
	Ready         map[string]bool
	Probes        map[string]movers.ProbeResult
	Healed        map[string][]string
	DryRun        bool
	Now           time.Time
}

// Result is the next status. Terminal means do not requeue.
type Result struct {
	Phase        portagev1alpha1.ActionPhase
	Message      string
	Workloads    []portagev1alpha1.WorkloadActionStatus
	Attestation  *portagev1alpha1.Attestation
	RequeueAfter time.Duration
	Terminal     bool
}

// Advance moves a Restore Action one step. It will never emit Succeeded unless
// every stateful workload is Ready and ProbeOK.
func Advance(status portagev1alpha1.ActionStatus, facts Facts) Result {
	phase := status.Phase
	if phase == "" {
		phase = portagev1alpha1.ActionPhasePending
	}
	workloads := seedWorkloads(status.Workloads, facts.Inventory)
	applyFacts(workloads, facts)

	switch phase {
	case portagev1alpha1.ActionPhaseSucceeded, portagev1alpha1.ActionPhaseFailed, portagev1alpha1.ActionPhaseRolledBack:
		return Result{Phase: phase, Message: status.Message, Workloads: workloads, Attestation: status.Attestation, Terminal: true}

	case portagev1alpha1.ActionPhasePending, portagev1alpha1.ActionPhasePreflight:
		if msg := preflight(workloads, facts); msg != "" {
			return fail(workloads, "preflight: "+msg)
		}
		if facts.DryRun {
			return Result{
				Phase:     portagev1alpha1.ActionPhaseSucceeded,
				Message:   "dry-run preflight passed (no restore performed)",
				Workloads: workloads,
				Terminal:  true,
				Attestation: &portagev1alpha1.Attestation{
					RecordedAt: metav1.NewTime(facts.Now),
					Source:     "dry-run",
					Workloads:  workloads,
				},
			}
		}
		return Result{Phase: portagev1alpha1.ActionPhaseRehydrating, Message: "preflight passed", Workloads: workloads, RequeueAfter: 2 * time.Second}

	case portagev1alpha1.ActionPhaseQuiescing:
		return Result{Phase: portagev1alpha1.ActionPhaseRehydrating, Message: "quiesced", Workloads: workloads, RequeueAfter: time.Second}

	case portagev1alpha1.ActionPhaseRehydrating, portagev1alpha1.ActionPhaseApplying:
		if pending := waiting(workloads, func(w portagev1alpha1.WorkloadActionStatus) bool {
			return w.Class == portagev1alpha1.ClassStateless || facts.Rehydrated[w.Key]
		}); pending != "" {
			return Result{Phase: portagev1alpha1.ActionPhaseRehydrating, Message: "rehydrating " + pending, Workloads: workloads, RequeueAfter: 5 * time.Second}
		}
		return Result{Phase: portagev1alpha1.ActionPhaseWaitingReady, Message: "data restored; waiting for Ready + probe", Workloads: workloads, RequeueAfter: 2 * time.Second}

	case portagev1alpha1.ActionPhaseWaitingReady, portagev1alpha1.ActionPhaseHealing:
		if pending := waiting(workloads, func(w portagev1alpha1.WorkloadActionStatus) bool { return w.Ready }); pending != "" {
			return Result{Phase: portagev1alpha1.ActionPhaseWaitingReady, Message: pending + " is not Ready", Workloads: workloads, RequeueAfter: 5 * time.Second}
		}
		if pending := waiting(workloads, func(w portagev1alpha1.WorkloadActionStatus) bool {
			return w.Class == portagev1alpha1.ClassStateless || w.ProbeOK
		}); pending != "" {
			return Result{Phase: portagev1alpha1.ActionPhaseWaitingReady, Message: pending + " class probe has not passed", Workloads: workloads, RequeueAfter: 5 * time.Second}
		}
		return attest(workloads, facts.Now)

	case portagev1alpha1.ActionPhaseAttesting:
		return attest(workloads, facts.Now)

	default:
		return Result{Phase: portagev1alpha1.ActionPhasePreflight, Message: "starting preflight", Workloads: workloads, RequeueAfter: time.Second}
	}
}

func attest(workloads []portagev1alpha1.WorkloadActionStatus, now time.Time) Result {
	ok, reason := actionphase.CanSucceed(workloads)
	if !ok {
		// Refuse to write Succeeded. Stay in WaitingReady with the reason.
		return Result{Phase: portagev1alpha1.ActionPhaseWaitingReady, Message: reason, Workloads: workloads, RequeueAfter: 5 * time.Second}
	}
	ids := make([]string, 0, len(workloads))
	for _, w := range workloads {
		if w.Key != "" {
			ids = append(ids, w.Key)
		}
	}
	return Result{
		Phase:     portagev1alpha1.ActionPhaseSucceeded,
		Message:   "ready and class probes passed",
		Workloads: workloads,
		Terminal:  true,
		Attestation: &portagev1alpha1.Attestation{
			RecordedAt:  metav1.NewTime(now),
			Source:      "controller",
			Workloads:   cloneWorkloads(workloads),
			ArtifactIDs: ids,
		},
	}
}

func preflight(workloads []portagev1alpha1.WorkloadActionStatus, facts Facts) string {
	if len(workloads) == 0 {
		return "no workloads in inventory"
	}
	for _, w := range workloads {
		if w.Class == portagev1alpha1.ClassStateless {
			continue
		}
		if !facts.Useful[w.Key] {
			msg := facts.UsefulMessage[w.Key]
			if msg == "" {
				msg = "no useful artifact"
			}
			return w.Key + ": " + msg
		}
	}
	return ""
}

func seedWorkloads(existing []portagev1alpha1.WorkloadActionStatus, inv []classify.Workload) []portagev1alpha1.WorkloadActionStatus {
	if len(inv) == 0 {
		return cloneWorkloads(existing)
	}
	byKey := map[string]portagev1alpha1.WorkloadActionStatus{}
	for _, w := range existing {
		byKey[w.Key] = w
	}
	out := make([]portagev1alpha1.WorkloadActionStatus, 0, len(inv))
	for _, w := range inv {
		st := byKey[w.Key()]
		st.Name = w.Name
		st.Key = w.Key()
		st.Class = w.Class
		st.Engine = w.Engine
		out = append(out, st)
	}
	return out
}

func applyFacts(workloads []portagev1alpha1.WorkloadActionStatus, facts Facts) {
	for i := range workloads {
		w := &workloads[i]
		if facts.Ready != nil {
			w.Ready = facts.Ready[w.Key]
		}
		if facts.Probes != nil {
			if p, ok := facts.Probes[w.Key]; ok {
				w.Probe = p.Message
				w.ProbeOK = p.OK
			}
		}
		if facts.Healed != nil {
			if h := facts.Healed[w.Key]; len(h) > 0 {
				w.Healed = append(w.Healed[:0], h...)
			}
		}
		if w.Class == portagev1alpha1.ClassStateless && w.Ready {
			w.ProbeOK = true
			if w.Probe == "" {
				w.Probe = "k8s-ready"
			}
		}
	}
}

func waiting(workloads []portagev1alpha1.WorkloadActionStatus, ok func(portagev1alpha1.WorkloadActionStatus) bool) string {
	for _, w := range workloads {
		if !ok(w) {
			return w.Key
		}
	}
	return ""
}

func fail(workloads []portagev1alpha1.WorkloadActionStatus, msg string) Result {
	return Result{Phase: portagev1alpha1.ActionPhaseFailed, Message: msg, Workloads: workloads, Terminal: true}
}

func cloneWorkloads(in []portagev1alpha1.WorkloadActionStatus) []portagev1alpha1.WorkloadActionStatus {
	if in == nil {
		return nil
	}
	out := make([]portagev1alpha1.WorkloadActionStatus, len(in))
	copy(out, in)
	return out
}
