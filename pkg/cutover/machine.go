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
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/actionphase"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/movers"
	"github.com/PipeOpsHQ/portage/pkg/restore"
)

// Default bounds. A Cutover that cannot finish must not hold the source
// frozen indefinitely (the v0.2.16 drill held it 3.5 h).
const (
	DefaultFreezeTimeout = 30 * time.Minute
	DefaultReadyTimeout  = 15 * time.Minute
)

// Condition types the controller records so phase deadlines survive restarts.
const (
	ConditionSourceFrozen    = "SourceFrozen"
	ConditionTrafficSwitched = "TrafficSwitched"
)

// Facts observed this reconcile.
type Facts struct {
	Inventory []classify.Workload
	Frozen    bool
	LagZero   bool
	// LagReason explains why LagZero is false (or what it was checked against).
	LagReason string
	// LagBlocked means dest cannot catch up without operator action
	// (e.g. ReplicationDestination paused). Cutover aborts instead of waiting.
	LagBlocked bool
	Promoted   bool
	Switched   bool
	Rolled     bool
	Ready      map[string]bool
	// Notes is a per-workload status message (why dest is not Ready).
	Notes    map[string]string
	Probes   map[string]movers.ProbeResult
	DryRun   bool
	Rollback bool
	// PreflightError refuses the Cutover before anything is frozen.
	PreflightError string
	// StartedAt, FrozenAt, SwitchedAt anchor the phase deadlines.
	StartedAt  time.Time
	FrozenAt   time.Time
	SwitchedAt time.Time
	// FreezeTimeout bounds source downtime before traffic moves
	// (Quiescing through Switching). ReadyTimeout bounds WaitingReady.
	// Zero means the package default.
	FreezeTimeout time.Duration
	ReadyTimeout  time.Duration
	Now           time.Time
}

// UnfreezeOnFail is true when a Failed result from phase prev must restore
// source replicas: traffic has not been confirmed on dest, so source is
// still the primary and leaving it frozen is pure downtime.
func UnfreezeOnFail(prev portagev1alpha1.ActionPhase) bool {
	switch prev {
	case portagev1alpha1.ActionPhaseQuiescing, portagev1alpha1.ActionPhaseCatchingUp,
		portagev1alpha1.ActionPhasePromoting, portagev1alpha1.ActionPhaseSwitching:
		return true
	}
	return false
}

func orDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

// freezeExpired reports whether the pre-switch freeze budget is spent.
func freezeExpired(facts Facts) (bool, time.Duration) {
	limit := orDefault(facts.FreezeTimeout, DefaultFreezeTimeout)
	since := facts.FrozenAt
	if since.IsZero() {
		since = facts.StartedAt
	}
	if since.IsZero() || facts.Now.IsZero() {
		return false, limit
	}
	return facts.Now.Sub(since) > limit, limit
}

func abort(workloads []portagev1alpha1.WorkloadActionStatus, msg string) restore.Result {
	return restore.Result{Phase: portagev1alpha1.ActionPhaseFailed, Message: "cutover aborted: " + msg + "; source unfrozen", Workloads: workloads, Terminal: true}
}

// Advance one Cutover step. Succeeded still requires Ready+probe.
func Advance(status portagev1alpha1.ActionStatus, facts Facts) restore.Result {
	phase := status.Phase
	if phase == "" {
		phase = portagev1alpha1.ActionPhasePending
	}
	workloads := seed(status.Workloads, facts)
	if facts.Rollback && phase != portagev1alpha1.ActionPhaseRolledBack {
		return restore.Result{Phase: portagev1alpha1.ActionPhaseRolledBack, Message: "source unfrozen; traffic rolled back", Workloads: workloads, Terminal: true}
	}
	switch phase {
	case portagev1alpha1.ActionPhaseSucceeded, portagev1alpha1.ActionPhaseFailed, portagev1alpha1.ActionPhaseRolledBack:
		return restore.Result{Phase: phase, Message: status.Message, Workloads: workloads, Attestation: status.Attestation, Terminal: true}
	case portagev1alpha1.ActionPhasePending, portagev1alpha1.ActionPhasePreflight:
		if facts.Rollback {
			return restore.Result{Phase: portagev1alpha1.ActionPhaseRolledBack, Message: "rollback requested", Workloads: workloads, Terminal: true}
		}
		if facts.PreflightError != "" {
			return restore.Result{Phase: portagev1alpha1.ActionPhaseFailed, Message: "preflight: " + facts.PreflightError, Workloads: workloads, Terminal: true}
		}
		if facts.DryRun {
			return restore.Result{Phase: portagev1alpha1.ActionPhaseSucceeded, Message: "dry-run cutover preflight", Workloads: workloads, Terminal: true}
		}
		return restore.Result{Phase: portagev1alpha1.ActionPhaseQuiescing, Message: "freezing source writes", Workloads: workloads, RequeueAfter: time.Second}
	case portagev1alpha1.ActionPhaseQuiescing, portagev1alpha1.ActionPhaseCatchingUp,
		portagev1alpha1.ActionPhasePromoting, portagev1alpha1.ActionPhaseSwitching:
		return advanceFrozen(phase, facts, workloads)
	case portagev1alpha1.ActionPhaseWaitingReady, portagev1alpha1.ActionPhaseHealing, portagev1alpha1.ActionPhaseAttesting:
		return advanceReady(facts, workloads)
	default:
		return restore.Result{Phase: portagev1alpha1.ActionPhasePreflight, Message: "starting cutover", Workloads: workloads, RequeueAfter: time.Second}
	}
}

// advanceFrozen covers the phases where source is frozen and traffic has
// not moved. Every one of them is bounded by FreezeTimeout.
func advanceFrozen(phase portagev1alpha1.ActionPhase, facts Facts, workloads []portagev1alpha1.WorkloadActionStatus) restore.Result {
	if phase == portagev1alpha1.ActionPhaseCatchingUp && facts.LagBlocked {
		return abort(workloads, "replica cannot catch up: "+facts.LagReason)
	}
	if expired, limit := freezeExpired(facts); expired {
		why := "phase " + string(phase) + " did not complete"
		if phase == portagev1alpha1.ActionPhaseCatchingUp && facts.LagReason != "" {
			why = facts.LagReason
		}
		return abort(workloads, fmt.Sprintf("freeze exceeded %s: %s", limit, why))
	}
	switch phase {
	case portagev1alpha1.ActionPhaseQuiescing:
		if !facts.Frozen {
			return restore.Result{Phase: portagev1alpha1.ActionPhaseQuiescing, Message: "waiting for source scale-to-zero", Workloads: workloads, RequeueAfter: 3 * time.Second}
		}
		return restore.Result{Phase: portagev1alpha1.ActionPhaseCatchingUp, Message: "source frozen; catching up replica", Workloads: workloads, RequeueAfter: time.Second}
	case portagev1alpha1.ActionPhaseCatchingUp:
		if !facts.LagZero {
			msg := "waiting for replica lag=0"
			if facts.LagReason != "" {
				msg += ": " + facts.LagReason
			}
			return restore.Result{Phase: portagev1alpha1.ActionPhaseCatchingUp, Message: msg, Workloads: workloads, RequeueAfter: 3 * time.Second}
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
	}
	return restore.Result{Phase: portagev1alpha1.ActionPhaseFailed, Message: "unexpected frozen phase " + string(phase), Workloads: workloads, Terminal: true}
}

// advanceReady waits for dest Ready + probe after traffic moved. It is
// bounded by ReadyTimeout; on timeout the Action fails but source stays
// frozen and traffic stays on dest, because dest may already hold writes.
func advanceReady(facts Facts, workloads []portagev1alpha1.WorkloadActionStatus) restore.Result {
	wait := func(msg string) restore.Result {
		limit := orDefault(facts.ReadyTimeout, DefaultReadyTimeout)
		if !facts.SwitchedAt.IsZero() && !facts.Now.IsZero() && facts.Now.Sub(facts.SwitchedAt) > limit {
			return restore.Result{
				Phase:     portagev1alpha1.ActionPhaseFailed,
				Message:   fmt.Sprintf("dest not Ready within %s of traffic switch: %s. Traffic is on dest and source is still frozen; set spec.rollback: true to fail back", limit, msg),
				Workloads: workloads,
				Terminal:  true,
			}
		}
		return restore.Result{Phase: portagev1alpha1.ActionPhaseWaitingReady, Message: msg, Workloads: workloads, RequeueAfter: 5 * time.Second}
	}
	for _, w := range workloads {
		if !w.Ready {
			msg := w.Key + " is not Ready on dest"
			if w.Message != "" {
				msg += ": " + w.Message
			}
			return wait(msg)
		}
		if w.Class != portagev1alpha1.ClassStateless && !w.ProbeOK {
			return wait(w.Key + " class probe has not passed")
		}
	}
	if ok, reason := actionphase.CanSucceed(workloads); !ok {
		return wait(reason)
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
		if n, ok := facts.Notes[w.Key()]; ok {
			st.Message = n
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
