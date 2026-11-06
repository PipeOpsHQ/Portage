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
	"os"
	"strings"
	"time"

	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/apply"
	"github.com/PipeOpsHQ/portage/pkg/backup"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/clusterobjects"
	"github.com/PipeOpsHQ/portage/pkg/clusters"
	"github.com/PipeOpsHQ/portage/pkg/cutover"
	"github.com/PipeOpsHQ/portage/pkg/dump"
	"github.com/PipeOpsHQ/portage/pkg/export"
	"github.com/PipeOpsHQ/portage/pkg/heal"
	"github.com/PipeOpsHQ/portage/pkg/kubeexec"
	"github.com/PipeOpsHQ/portage/pkg/metrics"
	"github.com/PipeOpsHQ/portage/pkg/movers"
	pgmover "github.com/PipeOpsHQ/portage/pkg/movers/postgres"
	"github.com/PipeOpsHQ/portage/pkg/movers/rclone"
	"github.com/PipeOpsHQ/portage/pkg/movers/volsync"
	whmover "github.com/PipeOpsHQ/portage/pkg/movers/webhook"
	"github.com/PipeOpsHQ/portage/pkg/objectstore"
	"github.com/PipeOpsHQ/portage/pkg/probe"
	"github.com/PipeOpsHQ/portage/pkg/render"
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
	Store   objectstore.Store
	// Resolve returns source and dest cluster clients. Nil ⇒ in-cluster for both.
	Resolve func(context.Context, *portagev1alpha1.ClusterPair) (clusters.Pair, error)
	Now     func() time.Time
}

func (r *ActionReconciler) store() objectstore.Store {
	if r.Store != nil {
		return r.Store
	}
	return objectstore.FromEnv()
}

func (r *ActionReconciler) endpoints(ctx context.Context, pair *portagev1alpha1.ClusterPair) (clusters.Pair, error) {
	loc := clusters.Local("local", r.Kube, r.Dynamic, r.Exec, nil)
	if pair != nil && pair.Spec.Source.Name != "" {
		loc.Name = pair.Spec.Source.Name
	}
	remoteDest := pair != nil && pair.Spec.Destination.HasRemoteAuth()
	if r.Resolve != nil {
		p, err := r.Resolve(ctx, pair)
		if err != nil {
			if remoteDest {
				return clusters.Pair{}, fmt.Errorf("resolve dest cluster: %w", err)
			}
			log.FromContext(ctx).Error(err, "resolve ClusterPair; using in-cluster")
			return clusters.Pair{Source: loc, Dest: loc}, nil
		}
		if p.Source.Kube == nil {
			p.Source = loc
		}
		if p.Dest.Kube == nil {
			if remoteDest {
				return clusters.Pair{}, fmt.Errorf("destination cluster %q client is empty", pair.Spec.Destination.Name)
			}
			p.Dest = p.Source
		}
		if remoteDest && p.Dest.Kube == p.Source.Kube {
			return clusters.Pair{}, fmt.Errorf("destination cluster %q resolved to the source cluster; refusing to write dest CRs in-cluster", pair.Spec.Destination.Name)
		}
		return p, nil
	}
	if remoteDest {
		return clusters.Pair{}, fmt.Errorf("clusterpair dest has remote auth but no resolver")
	}
	return clusters.Pair{Source: loc, Dest: loc}, nil
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
// +kubebuilder:rbac:groups=portage.io,resources=plugins,verbs=get;list;watch
// +kubebuilder:rbac:groups=portage.io,resources=policies,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationsources;replicationdestinations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=portage.io,resources=policies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps;secrets;services;serviceaccounts,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets;deployments;daemonsets;replicasets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings;clusterroles;clusterrolebindings,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses;networkpolicies,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=*,resources=*,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshotclasses,verbs=get;list;watch

func (r *ActionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	act := &portagev1alpha1.Action{}
	if err := r.Get(ctx, req.NamespacedName, act); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// spec.rollback on a finished Cutover is the documented failback. It
	// used to be dropped here for Succeeded and Failed Actions.
	cutoverRollback := act.Spec.Type == portagev1alpha1.ActionCutover && act.Spec.Rollback &&
		(act.Status.Phase == portagev1alpha1.ActionPhaseSucceeded || act.Status.Phase == portagev1alpha1.ActionPhaseFailed)
	switch {
	case cutoverRollback:
		logger.Info("rollback requested on finished cutover", "phase", act.Status.Phase)
		act.Status.CompletionTime = nil
	case act.Status.Phase == portagev1alpha1.ActionPhaseSucceeded:
		// Replicate is a live loop: dest must keep tracking source.
		if act.Spec.Type != portagev1alpha1.ActionReplicate || act.Spec.DryRun {
			return ctrl.Result{}, nil
		}
	case act.Status.Phase == portagev1alpha1.ActionPhaseRolledBack:
		return ctrl.Result{}, nil
	case act.Status.Phase == portagev1alpha1.ActionPhaseFailed:
		// A dest apply failure used to set Failed and stop. The namespace
		// (or admission) can appear later. Resume only that class of Restore.
		// Preflight, a missing Policy, and an unsupported type stay terminal.
		if act.Spec.Type != portagev1alpha1.ActionRestore || !restoreRetryable(act.Status.Message) {
			return ctrl.Result{}, nil
		}
		logger.Info("resuming failed restore", "message", act.Status.Message)
		act.Status.Phase = portagev1alpha1.ActionPhaseRehydrating
		act.Status.CompletionTime = nil
	}

	// Every pass logs before any call that can block. A worker that holds
	// the lease and then goes silent has nothing else an operator can see.
	logger.Info("reconcile action", "type", act.Spec.Type, "phase", act.Status.Phase, "name", act.Name)

	if r.Kube == nil {
		return retryLater(ctx, fmt.Errorf("kube client is not configured"))
	}

	pol := &portagev1alpha1.Policy{}
	if act.Spec.PolicyRef == "" {
		return r.fail(ctx, act, "spec.policyRef is required")
	}
	if err := r.Get(ctx, types.NamespacedName{Name: act.Spec.PolicyRef, Namespace: act.Namespace}, pol); err != nil {
		return r.fail(ctx, act, "policy: "+err.Error())
	}
	if act.Status.Phase == "" {
		act.Status.Phase = portagev1alpha1.ActionPhasePending
		act.Status.Message = "pending"
		if err := r.writeStatus(ctx, act); err != nil {
			return retryLater(ctx, fmt.Errorf("mark action pending: %w", err))
		}
	}

	pair, err := r.loadPair(ctx, pol)
	if err != nil {
		return r.fail(ctx, act, err.Error())
	}
	ep, err := r.endpoints(ctx, pair)
	if err != nil {
		return retryLater(ctx, fmt.Errorf("resolve ClusterPair: %w", err))
	}
	if ep.Source.Kube == nil {
		ep.Source.Kube, ep.Source.Dynamic, ep.Source.Exec = r.Kube, r.Dynamic, r.Exec
	}
	if ep.Dest.Kube == nil {
		if pair != nil && pair.Spec.Destination.HasRemoteAuth() {
			return retryLater(ctx, fmt.Errorf("destination cluster client is empty"))
		}
		ep.Dest = ep.Source
	}

	nss := classify.Namespaces(pol.Spec.Selector.Namespaces, pol.Namespace)
	inv, err := classify.Walk(ctx, ep.Source.Kube, nss)
	if err != nil {
		return retryLater(ctx, fmt.Errorf("classify: %w", err))
	}
	inv = withClusterObjects(pol, inv)

	if act.Status.StartTime == nil {
		t := metav1.NewTime(r.now())
		act.Status.StartTime = &t
	}

	var res restore.Result
	switch act.Spec.Type {
	case portagev1alpha1.ActionBackup:
		res, err = r.runBackup(ctx, act, pol, pair, ep, inv)
	case portagev1alpha1.ActionRestore:
		res, err = r.runRestore(ctx, act, pol, pair, ep, inv)
	case portagev1alpha1.ActionReplicate:
		res, err = r.runReplicate(ctx, act, pol, pair, ep, inv)
	case portagev1alpha1.ActionCutover:
		res, err = r.runCutover(ctx, act, pol, pair, ep, inv)
	default:
		return r.fail(ctx, act, "unsupported action type "+string(act.Spec.Type))
	}
	if err != nil {
		return retryLater(ctx, err)
	}
	if pair != nil && ep.Dest.Name != "" {
		res.Message = strings.TrimSpace(res.Message + "; dest=" + ep.Dest.Name)
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
	if err := r.writeStatus(ctx, act); err != nil {
		return retryLater(ctx, err)
	}
	if res.Terminal {
		metrics.ObserveAction(string(act.Spec.Type), string(res.Phase))
	}
	if res.Terminal || res.RequeueAfter == 0 {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: res.RequeueAfter}, nil
}

func (r *ActionReconciler) runBackup(ctx context.Context, _ *portagev1alpha1.Action, pol *portagev1alpha1.Policy, pair *portagev1alpha1.ClusterPair, ep clusters.Pair, inv classify.Inventory) (restore.Result, error) {
	reg := r.registry(ctx, pair, ep)
	snaps := snapshots.Client{Dynamic: ep.Source.Dynamic}
	arts := make([]portagev1alpha1.ArtifactHealth, 0, len(inv.Workloads))
	for _, w := range inv.Workloads {
		if w.Class == portagev1alpha1.ClassStateless {
			continue
		}
		if clusterobjects.Is(w) {
			key, n, usefulDump, msg, err := r.captureClusterObjects(ctx, pol, pair, ep, w)
			if err != nil {
				arts = append(arts, backup.FromArtifact(w, false, 0, "cluster-objects: "+err.Error(), ""))
				continue
			}
			ev := usefulness.ForWorkload(w, usefulness.Input{
				SizeBytes: n,
				Files:     []usefulness.File{{Path: "objects.json", SizeBytes: n}},
			})
			if msg != "" {
				ev.Message = msg
			}
			arts = append(arts, backup.FromArtifact(w, ev.Useful && usefulDump, ev.SizeBytes, ev.Message, key))
			continue
		}
		if name := moverOverride(pol, w, "backup"); name != "" {
			m, cap, err := reg.Select(ctx, w, name)
			if err != nil {
				arts = append(arts, backup.FromArtifact(w, false, 0, err.Error(), ""))
				continue
			}
			if m != nil && cap.Backup {
				art, berr := m.Backup(ctx, w, movers.ClusterHandle{Name: destName(pair)})
				if berr != nil {
					arts = append(arts, backup.FromArtifact(w, false, 0, berr.Error(), ""))
					continue
				}
				arts = append(arts, backup.FromArtifact(w, art.Useful, art.SizeBytes, art.Message, art.ID))
				continue
			}
		}
		if len(dump.Command(w.Engine)) == 0 {
			m, cap, err := reg.Select(ctx, w, "")
			if err == nil && m != nil && cap.Backup {
				art, berr := m.Backup(ctx, w, movers.ClusterHandle{Name: destName(pair)})
				if berr != nil {
					arts = append(arts, backup.FromArtifact(w, false, 0, berr.Error(), ""))
					continue
				}
				arts = append(arts, backup.FromArtifact(w, art.Useful, art.SizeBytes, art.Message, art.ID))
				continue
			}
		}
		_, _ = snaps.CreateForWorkload(ctx, w, "", r.now())
		ready, snapName, _ := snaps.LatestReady(ctx, w)
		size := int64(0)
		sizeMsg := ""
		id := ""
		if len(dump.Command(w.Engine)) > 0 {
			// Logical engines must be judged on the dump, not PGDATA `du`.
			// Empty Postgres datadir is tens of MiB and is the Velero trap.
			key, n, usefulDump, err := dump.Capture(ctx, ep.Source.Kube, ep.Source.Exec, r.store(), w, r.now())
			if err != nil {
				arts = append(arts, backup.FromArtifact(w, false, 0, "dump: "+err.Error(), ""))
				continue
			}
			size, id = n, key
			if usefulDump {
				sizeMsg = "object-store dump " + key
			}
		} else if ep.Source.Exec != nil {
			if n, msg, err := probe.LiveBytes(ctx, ep.Source.Kube, ep.Source.Exec, w); err == nil {
				size, sizeMsg = n, msg
			} else {
				sizeMsg = err.Error()
			}
		}
		files := []usefulness.File{}
		if id != "" {
			files = append(files, usefulness.File{Path: "dump.sql", SizeBytes: size})
		}
		ev := usefulness.ForWorkload(w, usefulness.Input{
			SizeBytes:     size,
			Files:         files,
			HasSnapshot:   snapName != "",
			SnapshotReady: ready,
		})
		if ev.Message == "" {
			ev.Message = sizeMsg
		}
		arts = append(arts, backup.FromArtifact(w, ev.Useful, ev.SizeBytes, ev.Message, id))
	}
	if err := r.writePolicyArtifacts(ctx, pol, inv, arts); err != nil {
		return restore.Result{}, err
	}
	for _, a := range arts {
		if strings.Contains(a.Message, "waiting for lastSyncTime") || strings.Contains(a.Message, "waiting for ReplicationSource") {
			br := backup.Advance(portagev1alpha1.ActionStatus{}, backup.Facts{
				Inventory: inv.Workloads,
				Artifacts: arts,
				Now:       r.now(),
			})
			return restore.Result{
				Phase:        portagev1alpha1.ActionPhaseCatchingUp,
				Message:      a.Workload + ": " + a.Message,
				Workloads:    br.Workloads,
				RequeueAfter: 15 * time.Second,
			}, nil
		}
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

func (r *ActionReconciler) runRestore(ctx context.Context, act *portagev1alpha1.Action, pol *portagev1alpha1.Policy, pair *portagev1alpha1.ClusterPair, ep clusters.Pair, inv classify.Inventory) (restore.Result, error) {
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
	reg := r.registry(ctx, pair, ep)
	snaps := snapshots.Client{Dynamic: ep.Dest.Dynamic}
	opt := rehydrateOpts(pol, pair)
	if act.Status.Phase == portagev1alpha1.ActionPhaseRehydrating || act.Status.Phase == portagev1alpha1.ActionPhaseApplying || act.Status.Phase == "" || act.Status.Phase == portagev1alpha1.ActionPhasePreflight {
		var rendered []*unstructured.Unstructured
		for _, w := range inv.Workloads {
			if clusterobjects.Is(w) {
				continue
			}
			objs, err := export.Workload(ctx, ep.Source.Kube, w)
			if err != nil {
				continue
			}
			out, err := render.For(pol, opt.Transform).Render(ctx, render.Request{Policy: pol, Pair: pair, SourceObjects: objs})
			if err != nil {
				continue
			}
			rendered = append(rendered, out...)
		}
		if err := apply.Typed(ctx, ep.Dest.Kube, rendered); err != nil {
			// Missing namespace, admission, and conflicts clear once the
			// Replicate path (or the operator) removes the blocker. Failed
			// would freeze the Action with completionTime set.
			msg := "dest apply: " + err.Error()
			if restoreRetryable(msg) {
				return restore.Result{
					Phase:        portagev1alpha1.ActionPhaseRehydrating,
					Message:      msg,
					Workloads:    act.Status.Workloads,
					RequeueAfter: 5 * time.Second,
				}, nil
			}
			return restore.Result{Phase: portagev1alpha1.ActionPhaseFailed, Message: msg, Terminal: true}, nil
		}
	}
	for _, w := range inv.Workloads {
		if a, ok := artByKey[w.Key()]; ok {
			facts.Useful[w.Key()] = a.Useful
			facts.UsefulMessage[w.Key()] = a.Message
		}
		if w.Class == portagev1alpha1.ClassStateless {
			facts.Useful[w.Key()] = true
			facts.Rehydrated[w.Key()] = true
			// The manifest is on dest. Ready is a separate read of that
			// object. Leaving it unset kept WaitingReady after the
			// Deployment was already Available.
			if ep.Dest.Kube == nil {
				facts.Ready[w.Key()] = false
				facts.UsefulMessage[w.Key()] = "dest client required"
				continue
			}
			ready, err := workloads.Ready(ctx, ep.Dest.Kube, w)
			if err != nil {
				facts.Ready[w.Key()] = false
				facts.UsefulMessage[w.Key()] = err.Error()
				continue
			}
			facts.Ready[w.Key()] = ready
			if ready {
				facts.UsefulMessage[w.Key()] = "dest Ready"
			} else {
				facts.UsefulMessage[w.Key()] = "dest not Ready"
			}
			continue
		}
		if !facts.Useful[w.Key()] {
			msg := facts.UsefulMessage[w.Key()]
			if msg == "" {
				msg = "no useful artifact"
			}
			if !strings.HasPrefix(msg, "skipped:") {
				msg = "skipped: " + msg
			}
			facts.UsefulMessage[w.Key()] = msg
			facts.Rehydrated[w.Key()] = true
			continue
		}
		if clusterobjects.Is(w) {
			if a, ok := artByKey[w.Key()]; ok && a.Useful && a.ArtifactID != "" {
				ok, msg, err := r.restoreClusterObjects(ctx, pair, ep, a.ArtifactID)
				if err != nil {
					facts.Rehydrated[w.Key()] = false
					facts.UsefulMessage[w.Key()] = err.Error()
				} else {
					facts.Rehydrated[w.Key()] = true
					facts.Ready[w.Key()] = ok
					facts.Probes[w.Key()] = movers.ProbeResult{OK: ok, Message: msg}
					if !ok {
						facts.UsefulMessage[w.Key()] = msg
					}
				}
			}
			continue
		}
		if name := moverOverride(pol, w, "restore"); name != "" {
			m, cap, err := reg.Select(ctx, w, name)
			if err != nil {
				facts.Rehydrated[w.Key()] = false
				facts.UsefulMessage[w.Key()] = err.Error()
				continue
			}
			if m != nil && cap.Restore {
				a := artByKey[w.Key()]
				rerr := m.Restore(ctx, w, movers.Artifact{ID: a.ArtifactID, Mover: name, SizeBytes: a.SizeBytes, Useful: a.Useful}, movers.ClusterHandle{Name: destName(pair)})
				if rerr != nil {
					facts.Rehydrated[w.Key()] = false
					facts.UsefulMessage[w.Key()] = rerr.Error()
					continue
				}
				facts.Rehydrated[w.Key()] = true
				r.healWorkloadOn(ctx, ep.Dest.Kube, w, opt.Transform, facts.Healed)
				ready, _ := workloads.Ready(ctx, ep.Dest.Kube, w)
				facts.Ready[w.Key()] = ready
				if ready && ep.Dest.Exec != nil {
					facts.Probes[w.Key()] = probe.Run(ctx, ep.Dest.Kube, ep.Dest.Exec, w)
				}
				continue
			}
		}
		rehydrated := true
		for _, pvcName := range w.PVCNames {
			done, healed, err := snaps.EnsurePVCFromSnapshot(ctx, ep.Dest.Kube, w, pvcName, opt)
			facts.Healed[w.Key()] = append(facts.Healed[w.Key()], healed...)
			if err != nil {
				if a, ok := artByKey[w.Key()]; ok && a.Useful {
					continue
				}
				rehydrated = false
				facts.UsefulMessage[w.Key()] = err.Error()
				continue
			}
			if !done {
				rehydrated = false
			}
		}
		if a, ok := artByKey[w.Key()]; ok && a.Useful && a.ArtifactID != "" {
			switch act.Status.Phase {
			case portagev1alpha1.ActionPhaseWaitingReady, portagev1alpha1.ActionPhaseHealing, portagev1alpha1.ActionPhaseAttesting:
				// Dump already applied to leave Rehydrating. Re-psql every
				// reconcile hangs dest exec (stdin never finishes).
			default:
				// Logical dump must land. Empty dest postgres is Ready+pg_isready.
				// A successful replay is recorded on the Action. The next
				// reconcile of a still-Rehydrating Action must not exec psql
				// again: a dropped exec session leaves the remote psql running,
				// and a second full replay is what operators see as a retry
				// against an already-restored database.
				if dumpApplied(act, w.Key()) {
					break
				}
				ready, _ := workloads.Ready(ctx, ep.Dest.Kube, w)
				if !ready {
					rehydrated = false
				} else if rerr := dump.Apply(ctx, ep.Dest.Kube, ep.Dest.Exec, r.store(), w, a.ArtifactID); rerr != nil {
					rehydrated = false
					facts.UsefulMessage[w.Key()] = rerr.Error()
				} else if err := r.rememberDumpApplied(ctx, act, w.Key()); err != nil {
					return restore.Result{}, err
				}
			}
		} else if len(w.PVCNames) == 0 {
			if a, ok := artByKey[w.Key()]; ok && a.Useful {
				rehydrated = true
			}
		}
		facts.Rehydrated[w.Key()] = rehydrated
		r.healWorkloadOn(ctx, ep.Dest.Kube, w, opt.Transform, facts.Healed)
		ready, err := workloads.Ready(ctx, ep.Dest.Kube, w)
		if err == nil {
			facts.Ready[w.Key()] = ready
		}
		if ready && ep.Dest.Exec != nil {
			facts.Probes[w.Key()] = probe.Run(ctx, ep.Dest.Kube, ep.Dest.Exec, w)
		}
	}
	return restore.Advance(act.Status, facts), nil
}

func (r *ActionReconciler) runReplicate(ctx context.Context, act *portagev1alpha1.Action, pol *portagev1alpha1.Policy, pair *portagev1alpha1.ClusterPair, ep clusters.Pair, inv classify.Inventory) (restore.Result, error) {
	reg := r.registry(ctx, pair, ep)
	src, dst := movers.ClusterHandle{Name: "source"}, movers.ClusterHandle{Name: "dest"}
	if pair != nil {
		src.Name, src.Address = pair.Spec.Source.Name, pair.Spec.Source.Address
		dst.Name, dst.Address = pair.Spec.Destination.Name, pair.Spec.Destination.Address
	}
	var failed string
	workloadsStatus := []portagev1alpha1.WorkloadActionStatus{}
	// Object graph before movers. A Capsule Tenant has to exist on dest
	// before the namespace create, and VolSync would otherwise create that
	// namespace first (bare, which Capsule will not adopt later).
	var clusterSt *portagev1alpha1.WorkloadActionStatus
	var clusterFailed string
	for _, w := range inv.Workloads {
		if !clusterobjects.Is(w) {
			continue
		}
		st := portagev1alpha1.WorkloadActionStatus{Name: w.Name, Key: w.Key(), Class: w.Class, Engine: w.Engine}
		ok, msg, err := r.syncClusterObjectsLive(ctx, pol, pair, ep)
		if err != nil {
			log.FromContext(ctx).Error(err, "sync cluster objects")
			clusterFailed = err.Error()
			st.Message = clusterFailed
		} else {
			st.Ready, st.ProbeOK, st.Probe, st.Message = ok, ok, msg, msg
			if !ok {
				clusterFailed = "waiting for replica sync: " + msg
			}
		}
		clusterSt = &st
	}
	for _, w := range inv.Workloads {
		st := portagev1alpha1.WorkloadActionStatus{Name: w.Name, Key: w.Key(), Class: w.Class, Engine: w.Engine}
		if w.Class == portagev1alpha1.ClassStateless {
			st = destStatelessStatus(ctx, ep.Dest.Kube, w)
			workloadsStatus = append(workloadsStatus, st)
			continue
		}
		if clusterobjects.Is(w) {
			if clusterSt != nil {
				workloadsStatus = append(workloadsStatus, *clusterSt)
			}
			if clusterFailed != "" {
				// A hard sync error replaces an earlier mover error. A not-yet
				// attested graph does not hide a mover failure recorded above.
				if failed == "" || !strings.HasPrefix(clusterFailed, "waiting for replica sync") {
					failed = clusterFailed
				}
			}
			continue
		}
		m, cap, err := reg.Select(ctx, w, moverOverride(pol, w, "replicate"))
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
			workloadsStatus = append(workloadsStatus, st)
			continue
		}
		pr, _ := m.Probe(ctx, w, dst)
		st.Probe, st.ProbeOK, st.Message = pr.Message, pr.OK, pr.Message
		st.Ready = pr.OK
		if !pr.OK && failed == "" {
			failed = "waiting for replica sync: " + pr.Message
		}
		workloadsStatus = append(workloadsStatus, st)
	}
	r.pruneVolSync(ctx, pol, ep, inv)
	if act.Spec.DryRun {
		return restore.Result{Phase: portagev1alpha1.ActionPhaseSucceeded, Message: "dry-run replicate", Workloads: workloadsStatus, Terminal: true}, nil
	}
	requeue := replicateRequeue(pol)
	if failed != "" {
		if failed == "no replicate-capable mover" {
			return restore.Result{Phase: portagev1alpha1.ActionPhaseFailed, Message: failed, Workloads: workloadsStatus, Terminal: true}, nil
		}
		msg := failed
		if !strings.HasPrefix(msg, "waiting for replica sync") {
			msg = "waiting for replica sync: " + msg
		}
		return restore.Result{Phase: portagev1alpha1.ActionPhaseCatchingUp, Message: msg, Workloads: workloadsStatus, RequeueAfter: requeue}, nil
	}
	return restore.Result{
		Phase:        portagev1alpha1.ActionPhaseCatchingUp,
		Message:      "replica lag=0; dest probed; live-sync",
		Workloads:    workloadsStatus,
		RequeueAfter: requeue,
		Attestation: &portagev1alpha1.Attestation{
			RecordedAt: metav1.NewTime(r.now()),
			Source:     "controller",
			Workloads:  workloadsStatus,
		},
	}, nil
}

// destStatelessStatus reports dest Ready only when the workload exists and
// has a Ready replica. Missing dest must not look healthy — Stateless is
// GitOps-deployed, not VolSync-moved, but kubectl describe action is the
// health signal.
func destStatelessStatus(ctx context.Context, kube kubernetes.Interface, w classify.Workload) portagev1alpha1.WorkloadActionStatus {
	st := portagev1alpha1.WorkloadActionStatus{Name: w.Name, Key: w.Key(), Class: w.Class, Engine: w.Engine}
	if kube == nil {
		st.Message = "dest client required"
		return st
	}
	ready, err := workloads.Ready(ctx, kube, w)
	if err != nil {
		st.Message = err.Error()
		return st
	}
	if !ready {
		st.Message = "dest not Ready"
		return st
	}
	st.Ready, st.ProbeOK = true, true
	st.Message = "dest Ready"
	return st
}

// dumpAppliedAnnotation lists workload keys whose logical dump has already
// been replayed. Newline-separated. A second psql against a live Postgres
// is not idempotent.
const dumpAppliedAnnotation = "portage.io/dump-applied"

func dumpApplied(act *portagev1alpha1.Action, key string) bool {
	if act == nil || key == "" || act.Annotations == nil {
		return false
	}
	for _, line := range strings.Split(act.Annotations[dumpAppliedAnnotation], "\n") {
		if line == key {
			return true
		}
	}
	return false
}

func (r *ActionReconciler) rememberDumpApplied(ctx context.Context, act *portagev1alpha1.Action, key string) error {
	objKey := client.ObjectKeyFromObject(act)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &portagev1alpha1.Action{}
		if err := r.Get(ctx, objKey, latest); err != nil {
			return err
		}
		if dumpApplied(latest, key) {
			act.SetResourceVersion(latest.GetResourceVersion())
			if act.Annotations == nil {
				act.Annotations = map[string]string{}
			}
			act.Annotations[dumpAppliedAnnotation] = latest.Annotations[dumpAppliedAnnotation]
			return nil
		}
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}
		cur := latest.Annotations[dumpAppliedAnnotation]
		if cur == "" {
			latest.Annotations[dumpAppliedAnnotation] = key
		} else {
			latest.Annotations[dumpAppliedAnnotation] = cur + "\n" + key
		}
		if err := r.Update(ctx, latest); err != nil {
			return err
		}
		act.SetResourceVersion(latest.GetResourceVersion())
		if act.Annotations == nil {
			act.Annotations = map[string]string{}
		}
		act.Annotations[dumpAppliedAnnotation] = latest.Annotations[dumpAppliedAnnotation]
		return nil
	})
}

func (r *ActionReconciler) pruneVolSync(ctx context.Context, pol *portagev1alpha1.Policy, ep clusters.Pair, inv classify.Inventory) {
	if ep.Source.Dynamic == nil {
		return
	}
	m := volsync.Mover{
		Dynamic:     ep.Source.Dynamic,
		DestDynamic: ep.Dest.Dynamic,
		Kube:        ep.Source.Kube,
		DestKube:    ep.Dest.Kube,
	}
	nss := classify.Namespaces(nil, "")
	if pol != nil {
		nss = classify.Namespaces(pol.Spec.Selector.Namespaces, pol.Namespace)
	}
	logger := log.FromContext(ctx)
	for _, ns := range nss {
		keep := map[string]struct{}{}
		for _, w := range inv.Workloads {
			if w.Namespace == ns && len(w.PVCNames) > 0 {
				keep[w.Name] = struct{}{}
			}
		}
		if err := m.Prune(ctx, ns, keep); err != nil {
			logger.Error(err, "prune volsync", "namespace", ns)
		}
	}
}

func replicateRequeue(pol *portagev1alpha1.Policy) time.Duration {
	const floor = 5 * time.Second
	if pol == nil || pol.Spec.Replicate.RPO == "" {
		return floor
	}
	d, err := time.ParseDuration(pol.Spec.Replicate.RPO)
	if err != nil || d <= 0 {
		return floor
	}
	if d < floor {
		return d
	}
	// RPO is target lag, not poll interval. Keep a live loop.
	if d > 15*time.Second {
		return 15 * time.Second
	}
	return d
}

// cutoverCallTimeout bounds each exec, probe, and per-workload API call in a
// Cutover pass. The reconcile ctx has no deadline, and one hung psql exec
// pinned the worker for that Action. A pinned worker never reaches the
// rollback branch, so a patched spec.rollback looked ignored (drill Bug 3).
const cutoverCallTimeout = 30 * time.Second

// cutoverObjectSyncTimeout bounds the live cluster-object sync (many calls).
const cutoverObjectSyncTimeout = 2 * time.Minute

func (r *ActionReconciler) runCutover(ctx context.Context, act *portagev1alpha1.Action, pol *portagev1alpha1.Policy, pair *portagev1alpha1.ClusterPair, ep clusters.Pair, inv classify.Inventory) (restore.Result, error) {
	logger := log.FromContext(ctx)
	phase := act.Status.Phase
	now := r.now()
	ev := traffic.Event{Policy: pol.Name, Source: "source", Destination: "dest"}
	if pair != nil {
		ev.Source, ev.Destination = pair.Spec.Source.Name, pair.Spec.Destination.Name
	}
	hook := traffic.Hook(traffic.Noop{})
	if pol.Spec.Cutover.TrafficHook != "" {
		hook = traffic.Webhook{URL: pol.Spec.Cutover.TrafficHook}
	}

	// Rollback runs before anything else in the pass: no exec, probe, or
	// cross-cluster sync can delay it.
	if act.Spec.Rollback {
		return r.rollbackCutover(ctx, act, ep, inv, hook, ev), nil
	}

	facts := cutover.Facts{
		Inventory:     inv.Workloads,
		DryRun:        act.Spec.DryRun,
		Now:           now,
		FrozenAt:      conditionTime(act.Status.Conditions, cutover.ConditionSourceFrozen),
		SwitchedAt:    conditionTime(act.Status.Conditions, cutover.ConditionTrafficSwitched),
		FreezeTimeout: parseDuration(act.Spec.FreezeTimeout),
		ReadyTimeout:  parseDuration(pol.Spec.Cutover.ReadyTimeout),
	}
	if act.Status.StartTime != nil {
		facts.StartedAt = act.Status.StartTime.Time
	}
	stateful := sourceStateful(inv)

	switch phase {
	case "", portagev1alpha1.ActionPhasePending, portagev1alpha1.ActionPhasePreflight:
		facts.PreflightError = cutoverPreflight(ctx, ep.Source.Kube, ep.Source.Exec, stateful)
		// Freeze only once preflight passed, and never on dry-run: nothing
		// would unfreeze it.
		if facts.PreflightError == "" && !act.Spec.DryRun {
			for _, w := range stateful {
				if err := scaleWithTimeout(ctx, ep.Source.Kube, w, 0); err != nil {
					logger.Error(err, "freeze source", "workload", w.Key())
				}
			}
		}
	case portagev1alpha1.ActionPhaseQuiescing:
		facts.Frozen = true
		for _, w := range stateful {
			_ = scaleWithTimeout(ctx, ep.Source.Kube, w, 0)
			cctx, cancel := context.WithTimeout(ctx, cutoverCallTimeout)
			if !workloads.ScaledToZero(cctx, ep.Source.Kube, w) {
				facts.Frozen = false
			}
			cancel()
		}
	case portagev1alpha1.ActionPhaseCatchingUp:
		facts.LagZero, facts.LagBlocked, facts.LagReason = r.cutoverLag(ctx, pair, ep, stateful, facts.FrozenAt)
	case portagev1alpha1.ActionPhasePromoting:
		facts.Promoted = r.promoteDest(ctx, pol, pair, ep, stateful)
	case portagev1alpha1.ActionPhaseSwitching:
		for _, w := range stateful {
			_ = scaleWithTimeout(ctx, ep.Dest.Kube, w, 1)
		}
		ev.Action = "switch"
		if err := hook.Switch(ctx, ev); err != nil {
			logger.Error(err, "traffic switch")
		} else {
			facts.Switched = true
		}
	case portagev1alpha1.ActionPhaseWaitingReady, portagev1alpha1.ActionPhaseHealing, portagev1alpha1.ActionPhaseAttesting:
		for _, w := range stateful {
			_ = scaleWithTimeout(ctx, ep.Dest.Kube, w, 1)
		}
		facts.Ready, facts.Probes, facts.Notes = r.destReadiness(ctx, pol, pair, ep, inv)
	}

	res := cutover.Advance(act.Status, facts)

	switch {
	case phase == portagev1alpha1.ActionPhaseQuiescing && res.Phase == portagev1alpha1.ActionPhaseCatchingUp:
		setCondition(act, cutover.ConditionSourceFrozen, metav1.ConditionTrue, "ScaledToZero", "source stateful workloads at 0 replicas", now)
	case phase == portagev1alpha1.ActionPhaseSwitching && res.Phase == portagev1alpha1.ActionPhaseWaitingReady:
		setCondition(act, cutover.ConditionTrafficSwitched, metav1.ConditionTrue, "HookSucceeded", "traffic hook switch returned 2xx", now)
	case res.Phase == portagev1alpha1.ActionPhaseFailed && cutover.UnfreezeOnFail(phase):
		logger.Info("cutover aborted; unfreezing source", "phase", phase, "reason", res.Message)
		if phase == portagev1alpha1.ActionPhaseSwitching {
			// The switch hook may have partly applied before it failed.
			ev.Action = "rollback"
			if err := hook.Rollback(ctx, ev); err != nil {
				logger.Error(err, "traffic rollback after aborted switch")
			}
		}
		r.unfreezeSource(ctx, ep, stateful)
		setCondition(act, cutover.ConditionSourceFrozen, metav1.ConditionFalse, "Aborted", "source replicas restored after abort", now)
	}
	return res, nil
}

// rollbackCutover fails back: traffic to source, then source unfrozen. If
// traffic may be on dest and the hook fails, source stays frozen and the
// pass requeues. Unfreezing then would leave two writable primaries.
func (r *ActionReconciler) rollbackCutover(ctx context.Context, act *portagev1alpha1.Action, ep clusters.Pair, inv classify.Inventory, hook traffic.Hook, ev traffic.Event) restore.Result {
	logger := log.FromContext(ctx)
	phase := act.Status.Phase
	logger.Info("cutover rollback requested", "phase", phase)
	ev.Action = "rollback"
	if err := hook.Rollback(ctx, ev); err != nil {
		logger.Error(err, "traffic rollback")
		if trafficMayBeOnDest(act) {
			if phase == "" {
				phase = portagev1alpha1.ActionPhasePending
			}
			return restore.Result{
				Phase:        phase,
				Message:      "rollback pending: traffic hook failed (" + err.Error() + "); source stays frozen until traffic is back on source",
				Workloads:    act.Status.Workloads,
				RequeueAfter: 10 * time.Second,
			}
		}
	}
	r.unfreezeSource(ctx, ep, sourceStateful(inv))
	setCondition(act, cutover.ConditionSourceFrozen, metav1.ConditionFalse, "RolledBack", "source replicas restored", r.now())
	return cutover.Advance(act.Status, cutover.Facts{Inventory: inv.Workloads, Rollback: true, Rolled: true, Now: r.now()})
}

// trafficMayBeOnDest is true once the switch hook has been called.
func trafficMayBeOnDest(act *portagev1alpha1.Action) bool {
	switch act.Status.Phase {
	case portagev1alpha1.ActionPhaseSwitching, portagev1alpha1.ActionPhaseWaitingReady,
		portagev1alpha1.ActionPhaseHealing, portagev1alpha1.ActionPhaseAttesting,
		portagev1alpha1.ActionPhaseSucceeded:
		return true
	}
	return !conditionTime(act.Status.Conditions, cutover.ConditionTrafficSwitched).IsZero()
}

// sourceStateful is what Cutover freezes: stateful workloads, excluding the
// cluster-object pseudo-workload.
func sourceStateful(inv classify.Inventory) []classify.Workload {
	var out []classify.Workload
	for _, w := range inv.Stateful() {
		if !clusterobjects.Is(w) {
			out = append(out, w)
		}
	}
	return out
}

func (r *ActionReconciler) unfreezeSource(ctx context.Context, ep clusters.Pair, stateful []classify.Workload) {
	logger := log.FromContext(ctx)
	for _, w := range stateful {
		cctx, cancel := context.WithTimeout(ctx, cutoverCallTimeout)
		if _, err := workloads.Unfreeze(cctx, ep.Source.Kube, w); err != nil {
			logger.Error(err, "unfreeze source", "workload", w.Key())
		}
		cancel()
	}
}

// cutoverPreflight refuses a Cutover before the freeze when the freeze
// restart would break the source. A Postgres pod with fsGroup crash-loops
// if kubelet re-applies group-read to server.key on that mount (drill
// Bug 4). fsGroup itself is allowed (marketplace uses it for tenant
// isolation): with OnRootMismatch, a matching volume root, and a key that
// is 0600 now, the restart is safe.
func cutoverPreflight(ctx context.Context, kube kubernetes.Interface, execer kubeexec.Interface, stateful []classify.Workload) string {
	if kube == nil {
		return ""
	}
	var bad []string
	for _, w := range stateful {
		spec := sourcePodTemplate(ctx, kube, w)
		if !heal.PostgresFSGroup(spec) {
			continue
		}
		if risk := postgresRestartRisk(ctx, kube, execer, w, spec.SecurityContext); risk != "" {
			bad = append(bad, w.Key()+": "+risk)
		}
	}
	if len(bad) == 0 {
		return ""
	}
	return "source Postgres would not survive the freeze restart: " + strings.Join(bad, "; ")
}

func sourcePodTemplate(ctx context.Context, kube kubernetes.Interface, w classify.Workload) *corev1.PodSpec {
	cctx, cancel := context.WithTimeout(ctx, cutoverCallTimeout)
	defer cancel()
	switch w.Kind {
	case "StatefulSet":
		if sts, err := kube.AppsV1().StatefulSets(w.Namespace).Get(cctx, w.Name, metav1.GetOptions{}); err == nil {
			return &sts.Spec.Template.Spec
		}
	case "Deployment":
		if dep, err := kube.AppsV1().Deployments(w.Namespace).Get(cctx, w.Name, metav1.GetOptions{}); err == nil {
			return &dep.Spec.Template.Spec
		}
	}
	return nil
}

func postgresRestartRisk(ctx context.Context, kube kubernetes.Interface, execer kubeexec.Interface, w classify.Workload, sc *corev1.PodSecurityContext) string {
	if risk := cutover.PolicyRisk(sc); risk != "" {
		return risk
	}
	if execer == nil {
		return "cannot verify server.key and volume root permissions: no exec client for the source cluster"
	}
	cctx, cancel := context.WithTimeout(ctx, cutoverCallTimeout)
	defer cancel()
	pod, container, err := workloads.FirstReadyPod(cctx, kube, w)
	if err != nil {
		return "cannot verify server.key: no Ready source pod: " + err.Error()
	}
	mount, subPath := dataMount(pod, container)
	res, err := execer.Exec(cctx, w.Namespace, pod.Name, container, cutover.PostgresRestartCheckCmd(mount))
	if err != nil {
		return "cannot verify server.key: " + err.Error() + " " + strings.TrimSpace(res.Stderr)
	}
	return cutover.RestartRisk(res.Stdout, *sc.FSGroup, mount != "" && !subPath)
}

// dataMount is the container's first PVC mount. subPath is true when the
// mount is a subdirectory, so the volume root is not visible to stat.
func dataMount(pod *corev1.Pod, container string) (string, bool) {
	pvcVolumes := map[string]bool{}
	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim != nil {
			pvcVolumes[v.Name] = true
		}
	}
	for _, c := range pod.Spec.Containers {
		if container != "" && c.Name != container {
			continue
		}
		for _, m := range c.VolumeMounts {
			if pvcVolumes[m.Name] {
				return m.MountPath, m.SubPath != "" || m.SubPathExpr != ""
			}
		}
	}
	return "", false
}

// cutoverLag aggregates per-workload lag. A Postgres streaming standby is
// checked by WAL replay; anything else (including a restored primary) by
// its VolSync pair against the freeze time.
func (r *ActionReconciler) cutoverLag(ctx context.Context, pair *portagev1alpha1.ClusterPair, ep clusters.Pair, stateful []classify.Workload, frozenAt time.Time) (zero, blocked bool, reason string) {
	pg := pgmover.Mover{Kube: ep.Source.Kube, Dest: ep.Dest.Kube, Exec: ep.Dest.Exec}
	vs := volsync.Mover{Dynamic: ep.Source.Dynamic, DestDynamic: ep.Dest.Dynamic, Kube: ep.Source.Kube, DestKube: ep.Dest.Kube, Transport: transportOf(pair), DestPath: destPath(pair)}
	zero = true
	note := func(key, msg string) {
		zero = false
		if reason == "" {
			reason = key + ": " + msg
		}
	}
	for _, w := range stateful {
		if w.Engine == "postgres" || w.Engine == "timescale" {
			cctx, cancel := context.WithTimeout(ctx, cutoverCallTimeout)
			n, standby, err := pg.ReplayLag(cctx, w)
			cancel()
			if standby {
				switch {
				case err != nil:
					note(w.Key(), err.Error())
				case n > 0:
					note(w.Key(), fmt.Sprintf("standby has %d WAL bytes left to replay", n))
				}
				continue
			}
		}
		cctx, cancel := context.WithTimeout(ctx, cutoverCallTimeout)
		lag, err := vs.CutoverLag(cctx, w, frozenAt)
		cancel()
		switch {
		case err != nil:
			note(w.Key(), err.Error())
		case lag.Blocked:
			blocked = true
			zero = false
			reason = w.Key() + ": " + lag.Reason
			return zero, blocked, reason
		case !lag.Zero:
			note(w.Key(), lag.Reason)
		}
	}
	return zero, blocked, reason
}

func (r *ActionReconciler) promoteDest(ctx context.Context, pol *portagev1alpha1.Policy, pair *portagev1alpha1.ClusterPair, ep clusters.Pair, stateful []classify.Workload) bool {
	logger := log.FromContext(ctx)
	reg := r.registry(ctx, pair, ep)
	pg := pgmover.Mover{Kube: ep.Source.Kube, Dest: ep.Dest.Kube, Exec: ep.Dest.Exec}
	promoted := true
	for _, w := range stateful {
		cctx, cancel := context.WithTimeout(ctx, cutoverCallTimeout)
		var err error
		m, cap, _ := reg.Select(cctx, w, moverOverride(pol, w, "replicate"))
		switch {
		case m != nil && cap.Replicate:
			err = m.Promote(cctx, w, movers.ClusterHandle{Name: "dest"})
		case w.Engine == "postgres" || w.Engine == "timescale":
			err = pg.Promote(cctx, w, movers.ClusterHandle{Name: "dest"})
		default:
			_ = workloads.Scale(cctx, ep.Dest.Kube, w, 1)
		}
		cancel()
		if err != nil {
			logger.Error(err, "promote dest", "workload", w.Key())
			promoted = false
		}
	}
	return promoted
}

// destReadiness evaluates every workload in the inventory on dest. It used
// to walk only inv.Stateful(), so a Stateless Deployment never got a Ready
// entry and WaitingReady waited forever (drill Bug 2).
func (r *ActionReconciler) destReadiness(ctx context.Context, pol *portagev1alpha1.Policy, pair *portagev1alpha1.ClusterPair, ep clusters.Pair, inv classify.Inventory) (map[string]bool, map[string]movers.ProbeResult, map[string]string) {
	ready := map[string]bool{}
	probes := map[string]movers.ProbeResult{}
	notes := map[string]string{}
	for _, w := range inv.Workloads {
		key := w.Key()
		switch {
		case clusterobjects.Is(w):
			cctx, cancel := context.WithTimeout(ctx, cutoverObjectSyncTimeout)
			ok, msg, err := r.syncClusterObjectsLive(cctx, pol, pair, ep)
			cancel()
			if err != nil {
				ok, msg = false, err.Error()
			}
			ready[key], probes[key], notes[key] = ok, movers.ProbeResult{OK: ok, Message: msg}, msg
		case w.Class == portagev1alpha1.ClassStateless:
			cctx, cancel := context.WithTimeout(ctx, cutoverCallTimeout)
			st := destStatelessStatus(cctx, ep.Dest.Kube, w)
			cancel()
			ready[key], notes[key] = st.Ready, st.Message
		default:
			cctx, cancel := context.WithTimeout(ctx, cutoverCallTimeout)
			ok, err := workloads.Ready(cctx, ep.Dest.Kube, w)
			if err != nil {
				notes[key] = err.Error()
			}
			ready[key] = ok
			if ok && ep.Dest.Exec != nil {
				probes[key] = probe.Run(cctx, ep.Dest.Kube, ep.Dest.Exec, w)
			}
			cancel()
		}
	}
	return ready, probes, notes
}

func scaleWithTimeout(ctx context.Context, kube kubernetes.Interface, w classify.Workload, n int32) error {
	if kube == nil {
		return fmt.Errorf("scale %s: no client", w.Key())
	}
	cctx, cancel := context.WithTimeout(ctx, cutoverCallTimeout)
	defer cancel()
	return workloads.Scale(cctx, kube, w, n)
}

func conditionTime(conds []metav1.Condition, t string) time.Time {
	c := meta.FindStatusCondition(conds, t)
	if c == nil || c.Status != metav1.ConditionTrue {
		return time.Time{}
	}
	return c.LastTransitionTime.Time
}

func setCondition(act *portagev1alpha1.Action, t string, status metav1.ConditionStatus, reason, msg string, now time.Time) {
	meta.SetStatusCondition(&act.Status.Conditions, metav1.Condition{
		Type: t, Status: status, Reason: reason, Message: msg,
		ObservedGeneration: act.Generation, LastTransitionTime: metav1.NewTime(now),
	})
}

// parseDuration returns 0 (use the default) for empty or invalid input.
func parseDuration(s string) time.Duration {
	if s == "" {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0
	}
	return d
}

func (r *ActionReconciler) healWorkloadOn(ctx context.Context, kube kubernetes.Interface, w classify.Workload, opt transform.Options, acc map[string][]string) {
	if kube == nil {
		return
	}
	switch w.Kind {
	case "StatefulSet":
		sts, err := kube.AppsV1().StatefulSets(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err != nil {
			return
		}
		if names := heal.PodSpec(&sts.Spec.Template.Spec); len(names) > 0 {
			_, _ = kube.AppsV1().StatefulSets(w.Namespace).Update(ctx, sts, metav1.UpdateOptions{})
			acc[w.Key()] = append(acc[w.Key()], names...)
		}
	case "Deployment":
		dep, err := kube.AppsV1().Deployments(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err != nil {
			return
		}
		if names := heal.PodSpec(&dep.Spec.Template.Spec); len(names) > 0 {
			_, _ = kube.AppsV1().Deployments(w.Namespace).Update(ctx, dep, metav1.UpdateOptions{})
			acc[w.Key()] = append(acc[w.Key()], names...)
		}
	}
}

func (r *ActionReconciler) loadPair(ctx context.Context, pol *portagev1alpha1.Policy) (*portagev1alpha1.ClusterPair, error) {
	if pol.Spec.ClusterPair == "" {
		return nil, nil
	}
	pair := &portagev1alpha1.ClusterPair{}
	if err := r.Get(ctx, types.NamespacedName{Name: pol.Spec.ClusterPair}, pair); err != nil {
		return nil, fmt.Errorf("clusterpair %s: %w", pol.Spec.ClusterPair, err)
	}
	return pair, nil
}

func moverOverride(pol *portagev1alpha1.Policy, w classify.Workload, op string) string {
	if pol == nil {
		return ""
	}
	if pol.Spec.MoverOverrides != nil {
		if n := pol.Spec.MoverOverrides[string(w.Class)]; n != "" {
			return n
		}
	}
	switch op {
	case "backup":
		return pol.Spec.Backup.Mover
	case "restore":
		return pol.Spec.Restore.Mover
	case "replicate":
		return pol.Spec.Replicate.Mover
	}
	return ""
}

func destName(pair *portagev1alpha1.ClusterPair) string {
	if pair != nil && pair.Spec.Destination.Name != "" {
		return pair.Spec.Destination.Name
	}
	return "dest"
}

func (r *ActionReconciler) registry(ctx context.Context, pair *portagev1alpha1.ClusterPair, ep clusters.Pair) *movers.Registry {
	reg := movers.NewRegistry()
	reg.Register(pgmover.Mover{Kube: ep.Source.Kube, Dest: ep.Dest.Kube, Exec: ep.Dest.Exec})
	t := transportOf(pair)
	path := destPath(pair)
	dyn := ep.Source.Dynamic
	if dyn == nil {
		dyn = r.Dynamic
	}
	destDyn := ep.Dest.Dynamic
	if destDyn == nil && !remotePair(ep) {
		destDyn = dyn
	}
	creds := objectstore.CredsFromEnv()
	snapClass := snapshotClassOf(pair)
	m := volsync.Mover{
		Dynamic: dyn, DestDynamic: destDyn,
		Kube: ep.Source.Kube, DestKube: ep.Dest.Kube,
		Transport: t, DestPath: path, Creds: creds,
		Schedule: os.Getenv("PORTAGE_VOLSYNC_SCHEDULE"), SnapshotClass: snapClass,
		StorageClassMap: storageClassMapOf(pair),
	}
	reg.Register(m)
	if t == portagev1alpha1.TransportObjectStore {
		rc := rclone.New(dyn, path)
		rc.DestDynamic = destDyn
		rc.Kube = ep.Source.Kube
		rc.DestKube = ep.Dest.Kube
		rc.Creds = creds
		rc.Schedule = m.Schedule
		rc.SnapshotClass = snapClass
		rc.StorageClassMap = m.StorageClassMap
		reg.Register(rc)
	}
	r.registerPlugins(ctx, reg)
	return reg
}

func (r *ActionReconciler) registerPlugins(ctx context.Context, reg *movers.Registry) {
	if r.Client == nil {
		return
	}
	list := &portagev1alpha1.PluginList{}
	if err := r.List(ctx, list); err != nil {
		return
	}
	for i := range list.Items {
		p := list.Items[i]
		if p.Spec.WebhookURL == "" {
			continue
		}
		if p.Spec.Type != "" && p.Spec.Type != portagev1alpha1.PluginMover {
			continue
		}
		reg.Register(whmover.Mover{Plugin: p})
	}
}

func transportOf(pair *portagev1alpha1.ClusterPair) portagev1alpha1.TransportType {
	if pair == nil || pair.Spec.Transport == "" {
		return portagev1alpha1.TransportObjectStore
	}
	return pair.Spec.Transport
}

func snapshotClassOf(pair *portagev1alpha1.ClusterPair) string {
	if pair == nil {
		return ""
	}
	for _, v := range pair.Spec.SnapshotClassMap {
		if v != "" {
			return v
		}
	}
	return ""
}

func remotePair(ep clusters.Pair) bool {
	return ep.Source.Kube != nil && ep.Dest.Kube != nil && ep.Source.Kube != ep.Dest.Kube
}

func storageClassMapOf(pair *portagev1alpha1.ClusterPair) map[string]string {
	if pair == nil {
		return nil
	}
	return pair.Spec.StorageClassMap
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
		opt.Transform.RuntimeClassMap = pair.Spec.RuntimeClassMap
	}
	_ = pol
	return opt
}

func (r *ActionReconciler) writePolicyArtifacts(ctx context.Context, pol *portagev1alpha1.Policy, inv classify.Inventory, arts []portagev1alpha1.ArtifactHealth) error {
	key := client.ObjectKeyFromObject(pol)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &portagev1alpha1.Policy{}
		if err := r.Get(ctx, key, latest); err != nil {
			return err
		}
		latest.Status.Artifacts = arts
		ok, missing := backup.Healthy(inv, arts)
		latest.Status.BackupHealthy = ok
		metrics.ObservePolicy(latest.Name, latest.Namespace, ok)
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
	})
}

func (r *ActionReconciler) writeStatus(ctx context.Context, act *portagev1alpha1.Action) error {
	desired := act.Status.DeepCopy()
	key := client.ObjectKeyFromObject(act)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &portagev1alpha1.Action{}
		if err := r.Get(ctx, key, latest); err != nil {
			return err
		}
		latest.Status = *desired
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		act.SetResourceVersion(latest.GetResourceVersion())
		return nil
	})
}

// restoreRetryable is a dest-side failure that can clear without a new
// Action. The stored message includes "; dest=<name>". Permanent preflight
// and spec errors stay terminal even when they contain "not found".
func restoreRetryable(msg string) bool {
	if msg == "" {
		return false
	}
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "preflight:"):
		return false
	case strings.Contains(lower, "no restorable"):
		return false
	case strings.Contains(lower, "no workloads"):
		return false
	case strings.Contains(lower, "unsupported action"):
		return false
	case strings.Contains(lower, "spec.policyref"):
		return false
	case strings.Contains(lower, "policy:"):
		return false
	case strings.Contains(lower, "dry-run"):
		return false
	}
	return strings.Contains(msg, "dest apply:") ||
		strings.Contains(lower, "not found") ||
		strings.Contains(lower, "non-owned tenant") ||
		strings.Contains(lower, "admission webhook") ||
		strings.Contains(lower, "the object has been modified")
}

func (r *ActionReconciler) fail(ctx context.Context, act *portagev1alpha1.Action, msg string) (ctrl.Result, error) {
	act.Status.Phase = portagev1alpha1.ActionPhaseFailed
	act.Status.Message = msg
	t := metav1.NewTime(r.now())
	act.Status.CompletionTime = &t
	if err := r.writeStatus(ctx, act); err != nil {
		return retryLater(ctx, err)
	}
	return ctrl.Result{}, nil
}

// SetupWithManager registers the reconciler twice. Replicate reconciles are
// long (two clusters, object sync). Backup and Restore use a separate queue
// so a live replica cannot hold every worker. Failure backoff caps at 30s;
// the default cap is about 16 minutes, which left phase "" with no log line.
func (r *ActionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := ctrl.NewControllerManagedBy(mgr).
		Named("action-replicate").
		For(&portagev1alpha1.Action{}).
		WithEventFilter(predicate.NewPredicateFuncs(replicateAction)).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 2,
			RateLimiter:             actionRateLimiter(),
		}).
		Complete(r); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("action").
		For(&portagev1alpha1.Action{}).
		WithEventFilter(predicate.NewPredicateFuncs(func(o client.Object) bool {
			return !replicateAction(o)
		})).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 4,
			RateLimiter:             actionRateLimiter(),
		}).
		Complete(r)
}

func replicateAction(o client.Object) bool {
	act, ok := o.(*portagev1alpha1.Action)
	return ok && act.Spec.Type == portagev1alpha1.ActionReplicate
}

func actionRateLimiter() workqueue.TypedRateLimiter[reconcile.Request] {
	return workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](time.Second, 30*time.Second),
		&workqueue.TypedBucketRateLimiter[reconcile.Request]{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
	)
}
