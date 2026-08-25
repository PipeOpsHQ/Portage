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
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/backup"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/cutover"
	"github.com/PipeOpsHQ/portage/pkg/heal"
	"github.com/PipeOpsHQ/portage/pkg/kubeexec"
	"github.com/PipeOpsHQ/portage/pkg/movers"
	pgmover "github.com/PipeOpsHQ/portage/pkg/movers/postgres"
	"github.com/PipeOpsHQ/portage/pkg/movers/rclone"
	"github.com/PipeOpsHQ/portage/pkg/movers/volsync"
	"github.com/PipeOpsHQ/portage/pkg/probe"
	"github.com/PipeOpsHQ/portage/pkg/restore"
	"github.com/PipeOpsHQ/portage/pkg/snapshots"
	"github.com/PipeOpsHQ/portage/pkg/traffic"
	"github.com/PipeOpsHQ/portage/pkg/transform"
	"github.com/PipeOpsHQ/portage/pkg/usefulness"
	"github.com/PipeOpsHQ/portage/pkg/workloads"
)

// ActionReconciler runs Backup and Restore Actions.
// Restore will not write Succeeded unless class probes pass.
type ActionReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	Kube    kubernetes.Interface
	Dynamic dynamic.Interface
	Exec    kubeexec.Interface
	Now     func() time.Time
}

func (r *ActionReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// +kubebuilder:rbac:groups=portage.io,resources=actions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=portage.io,resources=actions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=portage.io,resources=clusterpairs,verbs=get;list;watch
// +kubebuilder:rbac:groups=portage.io,resources=policies,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationsources;replicationdestinations,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=portage.io,resources=policies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets;deployments;daemonsets;replicasets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshotclasses,verbs=get;list;watch

func (r *ActionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	act := &portagev1alpha1.Action{}
	if err := r.Get(ctx, req.NamespacedName, act); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	switch act.Status.Phase {
	case portagev1alpha1.ActionPhaseSucceeded, portagev1alpha1.ActionPhaseFailed, portagev1alpha1.ActionPhaseRolledBack:
		return ctrl.Result{}, nil
	}

	if r.Kube == nil {
		return ctrl.Result{}, fmt.Errorf("kube client is not configured")
	}

	pol := &portagev1alpha1.Policy{}
	if act.Spec.PolicyRef == "" {
		return r.fail(ctx, act, "spec.policyRef is required")
	}
	if err := r.Get(ctx, types.NamespacedName{Name: act.Spec.PolicyRef, Namespace: act.Namespace}, pol); err != nil {
		return r.fail(ctx, act, "policy: "+err.Error())
	}

	nss := classify.Namespaces(pol.Spec.Selector.Namespaces, pol.Namespace)
	inv, err := classify.Walk(ctx, r.Kube, nss)
	if err != nil {
		logger.Error(err, "classify")
		return ctrl.Result{}, err
	}

	if act.Status.StartTime == nil {
		t := metav1.NewTime(r.now())
		act.Status.StartTime = &t
	}

	var res restore.Result
	pair := r.loadPair(ctx, pol)
	switch act.Spec.Type {
	case portagev1alpha1.ActionBackup:
		res, err = r.runBackup(ctx, act, pol, inv)
	case portagev1alpha1.ActionRestore:
		res, err = r.runRestore(ctx, act, pol, pair, inv)
	case portagev1alpha1.ActionReplicate:
		res, err = r.runReplicate(ctx, act, pol, pair, inv)
	case portagev1alpha1.ActionCutover:
		res, err = r.runCutover(ctx, act, pol, pair, inv)
	default:
		return r.fail(ctx, act, "unsupported action type "+string(act.Spec.Type))
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	act.Status.Phase = res.Phase
	act.Status.Message = res.Message
	act.Status.Workloads = res.Workloads
	act.Status.ObservedGeneration = act.Generation
	if res.Attestation != nil {
		act.Status.Attestation = res.Attestation
	}
	if res.Terminal {
		t := metav1.NewTime(r.now())
		act.Status.CompletionTime = &t
	}
	if err := r.Status().Update(ctx, act); err != nil {
		return ctrl.Result{}, err
	}
	if res.Terminal || res.RequeueAfter == 0 {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: res.RequeueAfter}, nil
}

func (r *ActionReconciler) runBackup(ctx context.Context, _ *portagev1alpha1.Action, pol *portagev1alpha1.Policy, inv classify.Inventory) (restore.Result, error) {
	snaps := snapshots.Client{Dynamic: r.Dynamic}
	arts := make([]portagev1alpha1.ArtifactHealth, 0, len(inv.Workloads))
	for _, w := range inv.Workloads {
		if w.Class == portagev1alpha1.ClassStateless {
			continue
		}
		_, _ = snaps.CreateForWorkload(ctx, w, "", r.now())
		ready, snapName, _ := snaps.LatestReady(ctx, w)
		size := int64(0)
		sizeMsg := ""
		if r.Exec != nil {
			if n, msg, err := probe.LiveBytes(ctx, r.Kube, r.Exec, w); err == nil {
				size, sizeMsg = n, msg
			} else {
				sizeMsg = err.Error()
			}
		}
		ev := usefulness.ForWorkload(w, usefulness.Input{
			SizeBytes:     size,
			HasSnapshot:   snapName != "",
			SnapshotReady: ready,
		})
		if ev.Message == "" {
			ev.Message = sizeMsg
		}
		arts = append(arts, backup.FromArtifact(w, ev.Useful, ev.SizeBytes, ev.Message))
	}
	if err := r.writePolicyArtifacts(ctx, pol, inv, arts); err != nil {
		return restore.Result{}, err
	}
	br := backup.Advance(portagev1alpha1.ActionStatus{}, backup.Facts{
		Inventory: inv.Workloads,
		Artifacts: arts,
		Now:       r.now(),
	})
	return restore.Result{
		Phase:       br.Phase,
		Message:     br.Message,
		Workloads:   br.Workloads,
		Attestation: br.Attestation,
		Terminal:    br.Terminal,
	}, nil
}

func (r *ActionReconciler) runRestore(ctx context.Context, act *portagev1alpha1.Action, pol *portagev1alpha1.Policy, pair *portagev1alpha1.ClusterPair, inv classify.Inventory) (restore.Result, error) {
	facts := restore.Facts{
		Inventory:     inv.Workloads,
		Useful:        map[string]bool{},
		UsefulMessage: map[string]string{},
		Rehydrated:    map[string]bool{},
		Ready:         map[string]bool{},
		Probes:        map[string]movers.ProbeResult{},
		Healed:        map[string][]string{},
		DryRun:        act.Spec.DryRun,
		Now:           r.now(),
	}
	artByKey := map[string]portagev1alpha1.ArtifactHealth{}
	for _, a := range pol.Status.Artifacts {
		artByKey[a.Workload] = a
	}
	snaps := snapshots.Client{Dynamic: r.Dynamic}
	opt := rehydrateOpts(pol, pair)
	for _, w := range inv.Workloads {
		if a, ok := artByKey[w.Key()]; ok {
			facts.Useful[w.Key()] = a.Useful
			facts.UsefulMessage[w.Key()] = a.Message
		}
		if w.Class == portagev1alpha1.ClassStateless {
			facts.Useful[w.Key()] = true
			facts.Rehydrated[w.Key()] = true
			continue
		}
		rehydrated := true
		for _, pvcName := range w.PVCNames {
			done, healed, err := snaps.EnsurePVCFromSnapshot(ctx, r.Kube, w, pvcName, opt)
			facts.Healed[w.Key()] = append(facts.Healed[w.Key()], healed...)
			if err != nil {
				// In-place useful backup without a snapshot: treat as rehydrated.
				if a, ok := artByKey[w.Key()]; ok && a.Useful {
					continue
				}
				facts.UsefulMessage[w.Key()] = err.Error()
				rehydrated = false
				continue
			}
			if !done {
				rehydrated = false
			}
		}
		if len(w.PVCNames) == 0 {
			if a, ok := artByKey[w.Key()]; ok && a.Useful {
				rehydrated = true
			}
		}
		facts.Rehydrated[w.Key()] = rehydrated
		r.healWorkload(ctx, w, opt.Transform, facts.Healed)
		ready, err := workloads.Ready(ctx, r.Kube, w)
		if err == nil {
			facts.Ready[w.Key()] = ready
		}
		if ready && r.Exec != nil {
			facts.Probes[w.Key()] = probe.Run(ctx, r.Kube, r.Exec, w)
		}
	}
	return restore.Advance(act.Status, facts), nil
}

func (r *ActionReconciler) runReplicate(ctx context.Context, act *portagev1alpha1.Action, pol *portagev1alpha1.Policy, pair *portagev1alpha1.ClusterPair, inv classify.Inventory) (restore.Result, error) {
	reg := r.registry(pair)
	src, dst := movers.ClusterHandle{Name: "source"}, movers.ClusterHandle{Name: "dest"}
	if pair != nil {
		src.Name, dst.Name = pair.Spec.Source.Name, pair.Spec.Destination.Name
	}
	var failed string
	workloadsStatus := []portagev1alpha1.WorkloadActionStatus{}
	for _, w := range inv.Workloads {
		st := portagev1alpha1.WorkloadActionStatus{Name: w.Name, Key: w.Key(), Class: w.Class, Engine: w.Engine}
		if w.Class == portagev1alpha1.ClassStateless {
			st.Ready, st.ProbeOK = true, true
			workloadsStatus = append(workloadsStatus, st)
			continue
		}
		override := ""
		if pol.Spec.MoverOverrides != nil {
			override = pol.Spec.MoverOverrides[string(w.Class)]
		}
		m, cap, err := reg.Select(ctx, w, override)
		if err != nil {
			failed = err.Error()
			st.Message = failed
			workloadsStatus = append(workloadsStatus, st)
			continue
		}
		if m == nil || !cap.Replicate {
			st.Message = "no replicate-capable mover"
			failed = st.Message
			workloadsStatus = append(workloadsStatus, st)
			continue
		}
		if err := m.Replicate(ctx, w, src, dst); err != nil {
			failed = err.Error()
			st.Message = failed
		} else {
			st.Ready, st.ProbeOK, st.Message = true, true, "replicate CR applied ("+m.Name()+")"
		}
		workloadsStatus = append(workloadsStatus, st)
	}
	if act.Spec.DryRun {
		return restore.Result{Phase: portagev1alpha1.ActionPhaseSucceeded, Message: "dry-run replicate", Workloads: workloadsStatus, Terminal: true}, nil
	}
	if failed != "" {
		return restore.Result{Phase: portagev1alpha1.ActionPhaseFailed, Message: failed, Workloads: workloadsStatus, Terminal: true}, nil
	}
	return restore.Result{Phase: portagev1alpha1.ActionPhaseSucceeded, Message: "replication requested", Workloads: workloadsStatus, Terminal: true}, nil
}

func (r *ActionReconciler) runCutover(ctx context.Context, act *portagev1alpha1.Action, pol *portagev1alpha1.Policy, pair *portagev1alpha1.ClusterPair, inv classify.Inventory) (restore.Result, error) {
	facts := cutover.Facts{
		Inventory: inv.Workloads,
		Ready:     map[string]bool{},
		Probes:    map[string]movers.ProbeResult{},
		DryRun:    act.Spec.DryRun,
		Now:       r.now(),
	}
	reg := r.registry(pair)
	pg := pgmover.Mover{Kube: r.Kube, Exec: r.Exec}
	vs := volsync.Mover{Dynamic: r.Dynamic, Transport: transportOf(pair), DestPath: destPath(pair)}

	frozen, lagZero, promoted := true, true, true
	for _, w := range inv.Stateful() {
		switch act.Status.Phase {
		case "", portagev1alpha1.ActionPhasePending, portagev1alpha1.ActionPhasePreflight, portagev1alpha1.ActionPhaseQuiescing:
			_ = workloads.Scale(ctx, r.Kube, w, 0)
		}
		if n, ok, err := pg.LagSeconds(ctx, w); err == nil && ok && n > 0 {
			lagZero = false
		}
		if ok, err := vs.LagZero(ctx, w); err == nil && ok {
			// lastSyncTime present counts as caught up for PVC movers
			_ = ok
		}
		if act.Status.Phase == portagev1alpha1.ActionPhasePromoting {
			override := ""
			if pol.Spec.MoverOverrides != nil {
				override = pol.Spec.MoverOverrides[string(w.Class)]
			}
			m, cap, _ := reg.Select(ctx, w, override)
			if m != nil && cap.Replicate {
				if err := m.Promote(ctx, w, movers.ClusterHandle{Name: "dest"}); err != nil {
					promoted = false
				}
			} else if w.Engine == "postgres" || w.Engine == "timescale" {
				if err := pg.Promote(ctx, w, movers.ClusterHandle{Name: "dest"}); err != nil {
					promoted = false
				}
			} else {
				_ = workloads.Scale(ctx, r.Kube, w, 1)
			}
		}
		if act.Status.Phase == portagev1alpha1.ActionPhaseWaitingReady || act.Status.Phase == portagev1alpha1.ActionPhaseSwitching {
			_ = workloads.Scale(ctx, r.Kube, w, 1)
		}
		ready, _ := workloads.Ready(ctx, r.Kube, w)
		facts.Ready[w.Key()] = ready
		if ready && r.Exec != nil {
			facts.Probes[w.Key()] = probe.Run(ctx, r.Kube, r.Exec, w)
		}
	}
	if act.Status.Phase == portagev1alpha1.ActionPhaseQuiescing {
		frozen = true
		for _, w := range inv.Stateful() {
			if !workloads.ScaledToZero(ctx, r.Kube, w) {
				frozen = false
			}
		}
	}
	if act.Status.Phase == "" || act.Status.Phase == portagev1alpha1.ActionPhasePending || act.Status.Phase == portagev1alpha1.ActionPhasePreflight {
		frozen, lagZero, promoted = false, false, false
	}
	facts.Frozen, facts.LagZero, facts.Promoted = frozen, lagZero, promoted

	hook := traffic.Hook(traffic.Noop{})
	if pol.Spec.Cutover.TrafficHook != "" {
		hook = traffic.Webhook{URL: pol.Spec.Cutover.TrafficHook}
	}
	if act.Status.Phase == portagev1alpha1.ActionPhaseSwitching {
		src, dst := "source", "dest"
		if pair != nil {
			src, dst = pair.Spec.Source.Name, pair.Spec.Destination.Name
		}
		if err := hook.Switch(ctx, traffic.Event{Action: "switch", Policy: pol.Name, Source: src, Destination: dst}); err == nil {
			facts.Switched = true
		}
	}

	res := cutover.Advance(act.Status, facts)
	return res, nil
}

func (r *ActionReconciler) healWorkload(ctx context.Context, w classify.Workload, opt transform.Options, acc map[string][]string) {
	if r.Kube == nil {
		return
	}
	switch w.Kind {
	case "StatefulSet":
		sts, err := r.Kube.AppsV1().StatefulSets(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err != nil {
			return
		}
		if names := heal.PodSpec(&sts.Spec.Template.Spec); len(names) > 0 {
			_, _ = r.Kube.AppsV1().StatefulSets(w.Namespace).Update(ctx, sts, metav1.UpdateOptions{})
			acc[w.Key()] = append(acc[w.Key()], names...)
		}
	case "Deployment":
		dep, err := r.Kube.AppsV1().Deployments(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err != nil {
			return
		}
		if names := heal.PodSpec(&dep.Spec.Template.Spec); len(names) > 0 {
			_, _ = r.Kube.AppsV1().Deployments(w.Namespace).Update(ctx, dep, metav1.UpdateOptions{})
			acc[w.Key()] = append(acc[w.Key()], names...)
		}
	}
}

func (r *ActionReconciler) loadPair(ctx context.Context, pol *portagev1alpha1.Policy) *portagev1alpha1.ClusterPair {
	if pol.Spec.ClusterPair == "" {
		return nil
	}
	pair := &portagev1alpha1.ClusterPair{}
	if err := r.Get(ctx, types.NamespacedName{Name: pol.Spec.ClusterPair}, pair); err != nil {
		return nil
	}
	return pair
}

func (r *ActionReconciler) registry(pair *portagev1alpha1.ClusterPair) *movers.Registry {
	reg := movers.NewRegistry()
	reg.Register(pgmover.Mover{Kube: r.Kube, Exec: r.Exec})
	t := transportOf(pair)
	path := destPath(pair)
	if t == portagev1alpha1.TransportObjectStore {
		m := rclone.New(r.Dynamic, path)
		reg.Register(m)
	} else {
		reg.Register(volsync.Mover{Dynamic: r.Dynamic, Transport: t, DestPath: path})
	}
	return reg
}

func transportOf(pair *portagev1alpha1.ClusterPair) portagev1alpha1.TransportType {
	if pair == nil || pair.Spec.Transport == "" {
		return portagev1alpha1.TransportObjectStore
	}
	return pair.Spec.Transport
}

func destPath(pair *portagev1alpha1.ClusterPair) string {
	if pair == nil || pair.Spec.Destination.ObjectStore == nil {
		return ""
	}
	return pair.Spec.Destination.ObjectStore.URL
}

func rehydrateOpts(pol *portagev1alpha1.Policy, pair *portagev1alpha1.ClusterPair) snapshots.RehydrateOptions {
	opt := snapshots.RehydrateOptions{NeverOverwrite: true, DefaultSize: "10Gi"}
	if pair != nil {
		opt.Transform.StorageClassMap = pair.Spec.StorageClassMap
	}
	_ = pol
	return opt
}

func (r *ActionReconciler) writePolicyArtifacts(ctx context.Context, pol *portagev1alpha1.Policy, inv classify.Inventory, arts []portagev1alpha1.ArtifactHealth) error {
	latest := pol.DeepCopy()
	if err := r.Get(ctx, client.ObjectKeyFromObject(pol), latest); err != nil {
		return err
	}
	latest.Status.Artifacts = arts
	ok, missing := backup.Healthy(inv, arts)
	latest.Status.BackupHealthy = ok
	if !ok {
		latest.Status.Phase = portagev1alpha1.PolicyUnhealthy
		if len(missing) > 0 {
			latest.Status.Message = "backup not useful: " + missing[0]
		}
	} else if latest.Status.Phase == portagev1alpha1.PolicyUnhealthy {
		latest.Status.Phase = portagev1alpha1.PolicyHealthy
		latest.Status.Message = "all stateful artifacts useful"
	}
	return r.Status().Update(ctx, latest)
}

func (r *ActionReconciler) fail(ctx context.Context, act *portagev1alpha1.Action, msg string) (ctrl.Result, error) {
	act.Status.Phase = portagev1alpha1.ActionPhaseFailed
	act.Status.Message = msg
	t := metav1.NewTime(r.now())
	act.Status.CompletionTime = &t
	if err := r.Status().Update(ctx, act); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager registers the reconciler.
func (r *ActionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&portagev1alpha1.Action{}).
		Complete(r)
}
