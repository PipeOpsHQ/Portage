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

// Package cutover is the planned failover state machine.
// Warm replica (Replicate Action) is assumed already running.
package cutover

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/actionphase"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/movers"
	"github.com/PipeOpsHQ/portage/pkg/restore"
)

// Facts observed this reconcile.
type Facts struct {
	Inventory []classify.Workload
	Frozen    bool
	LagZero   bool
	Promoted  bool
	Switched  bool
	Ready     map[string]bool
	Probes    map[string]movers.ProbeResult
	DryRun    bool
	Now       time.Time
}

// Advance one Cutover step. Succeeded still requires Ready+probe.
func Advance(status portagev1alpha1.ActionStatus, facts Facts) restore.Result {
	phase := status.Phase
	if phase == "" {
		phase = portagev1alpha1.ActionPhasePending
	}
	workloads := seed(status.Workloads, facts)
	switch phase {
	case portagev1alpha1.ActionPhaseSucceeded, portagev1alpha1.ActionPhaseFailed, portagev1alpha1.ActionPhaseRolledBack:
		return restore.Result{Phase: phase, Message: status.Message, Workloads: workloads, Attestation: status.Attestation, Terminal: true}
	case portagev1alpha1.ActionPhasePending, portagev1alpha1.ActionPhasePreflight:
		if facts.DryRun {
			return restore.Result{Phase: portagev1alpha1.ActionPhaseSucceeded, Message: "dry-run cutover preflight", Workloads: workloads, Terminal: true}
		}
		return restore.Result{Phase: portagev1alpha1.ActionPhaseQuiescing, Message: "freezing source writes", Workloads: workloads, RequeueAfter: time.Second}
	case portagev1alpha1.ActionPhaseQuiescing:
		if !facts.Frozen {
			return restore.Result{Phase: portagev1alpha1.ActionPhaseQuiescing, Message: "waiting for source scale-to-zero", Workloads: workloads, RequeueAfter: 3 * time.Second}
		}
		return restore.Result{Phase: portagev1alpha1.ActionPhaseCatchingUp, Message: "source frozen; catching up replica", Workloads: workloads, RequeueAfter: time.Second}
	case portagev1alpha1.ActionPhaseCatchingUp:
		if !facts.LagZero {
			return restore.Result{Phase: portagev1alpha1.ActionPhaseCatchingUp, Message: "waiting for replica lag=0", Workloads: workloads, RequeueAfter: 3 * time.Second}
		}
		return restore.Result{Phase: portagev1alpha1.ActionPhasePromoting, Message: "lag=0; promoting dest", Workloads: workloads, RequeueAfter: time.Second}
	case portagev1alpha1.ActionPhasePromoting:
		if !facts.Promoted {
			return restore.Result{Phase: portagev1alpha1.ActionPhasePromoting, Message: "promoting dest primary", Workloads: workloads, RequeueAfter: 3 * time.Second}
		}
		return restore.Result{Phase: portagev1alpha1.ActionPhaseSwitching, Message: "promoted; switching traffic", Workloads: workloads, RequeueAfter: time.Second}
	case portagev1alpha1.ActionPhaseSwitching:
		if !facts.Switched {
			return restore.Result{Phase: portagev1alpha1.ActionPhaseSwitching, Message: "waiting for traffic hook", Workloads: workloads, RequeueAfter: 2 * time.Second}
		}
		return restore.Result{Phase: portagev1alpha1.ActionPhaseWaitingReady, Message: "traffic switched; waiting for dest Ready + probe", Workloads: workloads, RequeueAfter: 2 * time.Second}
	case portagev1alpha1.ActionPhaseWaitingReady, portagev1alpha1.ActionPhaseHealing, portagev1alpha1.ActionPhaseAttesting:
		for _, w := range workloads {
			if !w.Ready {
				return restore.Result{Phase: portagev1alpha1.ActionPhaseWaitingReady, Message: w.Key + " is not Ready", Workloads: workloads, RequeueAfter: 5 * time.Second}
			}
			if w.Class != portagev1alpha1.ClassStateless && !w.ProbeOK {
				return restore.Result{Phase: portagev1alpha1.ActionPhaseWaitingReady, Message: w.Key + " class probe has not passed", Workloads: workloads, RequeueAfter: 5 * time.Second}
			}
		}
		ok, reason := actionphase.CanSucceed(workloads)
		if !ok {
			return restore.Result{Phase: portagev1alpha1.ActionPhaseWaitingReady, Message: reason, Workloads: workloads, RequeueAfter: 5 * time.Second}
		}
		return restore.Result{
			Phase:     portagev1alpha1.ActionPhaseSucceeded,
			Message:   "cutover complete; dest Ready and probed",
			Workloads: workloads,
			Terminal:  true,
			Attestation: &portagev1alpha1.Attestation{
				RecordedAt: metav1.NewTime(facts.Now),
				Source:     "cutover",
				Workloads:  workloads,
			},
		}
	default:
		return restore.Result{Phase: portagev1alpha1.ActionPhasePreflight, Message: "starting cutover", Workloads: workloads, RequeueAfter: time.Second}
	}
}

func seed(existing []portagev1alpha1.WorkloadActionStatus, facts Facts) []portagev1alpha1.WorkloadActionStatus {
	if len(facts.Inventory) == 0 {
		return existing
	}
	byKey := map[string]portagev1alpha1.WorkloadActionStatus{}
	for _, w := range existing {
		byKey[w.Key] = w
	}
	out := make([]portagev1alpha1.WorkloadActionStatus, 0, len(facts.Inventory))
	for _, w := range facts.Inventory {
		st := byKey[w.Key()]
		st.Name, st.Key, st.Class, st.Engine = w.Name, w.Key(), w.Class, w.Engine
		if facts.Ready != nil {
			st.Ready = facts.Ready[w.Key()]
		}
		if facts.Probes != nil {
			if p, ok := facts.Probes[w.Key()]; ok {
				st.Probe, st.ProbeOK = p.Message, p.OK
			}
		}
		if st.Class == portagev1alpha1.ClassStateless && st.Ready {
			st.ProbeOK = true
		}
		out = append(out, st)
	}
	return out
}
