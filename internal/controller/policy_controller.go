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

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/backup"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/clusters"
)

// PolicyReconciler classifies covered namespaces and writes inventory status.
// Backup/replicate/restore loops attach here as movers land.
type PolicyReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	KubeClient kubernetes.Interface
	// Resolve returns source and dest cluster clients. Nil ⇒ in-cluster for both.
	Resolve func(context.Context, *portagev1alpha1.ClusterPair) (clusters.Pair, error)
	Now     func() time.Time
}

// +kubebuilder:rbac:groups=portage.io,resources=actions,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=portage.io,resources=policies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=portage.io,resources=policies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=portage.io,resources=policies/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=statefulsets;deployments;daemonsets,verbs=get;list;watch

func (r *PolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	pol := &portagev1alpha1.Policy{}
	if err := r.Get(ctx, req.NamespacedName, pol); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if r.KubeClient == nil {
		return ctrl.Result{}, fmt.Errorf("kube client is not configured")
	}

	pair := r.loadPair(ctx, pol)
	srcKube, dstKube := r.KubeClient, r.KubeClient
	if r.Resolve != nil {
		if p, err := r.Resolve(ctx, pair); err == nil {
			if p.Source.Kube != nil {
				srcKube = p.Source.Kube
			}
			if p.Dest.Kube != nil {
				dstKube = p.Dest.Kube
			}
		}
	}

	nss := classify.Namespaces(pol.Spec.Selector.Namespaces, pol.Namespace)
	inv, err := classify.Walk(ctx, srcKube, nss)
	if err != nil {
		logger.Error(err, "classify")
		pol.Status.Phase = portagev1alpha1.PolicyUnhealthy
		pol.Status.Message = err.Error()
		_ = r.Status().Update(ctx, pol)
		return ctrl.Result{}, err
	}
	inv = withClusterObjects(pol, inv)

	items := make([]portagev1alpha1.InventoryItem, 0, len(inv.Workloads))
	unclassified := 0
	stateful := 0
	for _, w := range inv.Workloads {
		if w.Class != portagev1alpha1.ClassStateless {
			stateful++
		}
		if w.Unclassified {
			unclassified++
		}
		items = append(items, portagev1alpha1.InventoryItem{
			Namespace:    w.Namespace,
			Name:         w.Name,
			Kind:         w.Kind,
			Class:        w.Class,
			Engine:       w.Engine,
			PVCNames:     w.PVCNames,
			Unclassified: w.Unclassified,
		})
	}

	pol.Status.Inventory = items
	pol.Status.ObservedGeneration = pol.Generation

	invForHealth := classify.Inventory{}
	for _, it := range items {
		invForHealth.Workloads = append(invForHealth.Workloads, classify.Workload{
			Namespace: it.Namespace, Name: it.Name, Kind: it.Kind, Class: it.Class, Engine: it.Engine,
		})
	}
	healthy, missing := backup.Healthy(invForHealth, pol.Status.Artifacts)
	pol.Status.BackupHealthy = healthy

	switch {
	case stateful > 0 && !healthy:
		pol.Status.Phase = portagev1alpha1.PolicyUnhealthy
		msg := "stateful workloads have no useful backup"
		if len(missing) > 0 {
			msg = "backup not useful: " + missing[0]
		}
		pol.Status.Message = msg
	case unclassified > 0:
		pol.Status.Phase = portagev1alpha1.PolicyDegraded
		pol.Status.Message = fmt.Sprintf("%d workloads (%d stateful, %d unclassified)", len(items), stateful, unclassified)
	default:
		pol.Status.Phase = portagev1alpha1.PolicyHealthy
		pol.Status.Message = fmt.Sprintf("%d workloads (%d stateful)", len(items), stateful)
	}

	if err := r.Status().Update(ctx, pol); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.maybeAutoRestore(ctx, pol, inv, dstKube); err != nil {
		logger.Error(err, "auto-restore")
	}
	if err := r.maybeBackup(ctx, pol); err != nil {
		logger.Error(err, "rpo backup")
	}
	if err := r.maybeReplicate(ctx, pol); err != nil {
		logger.Error(err, "live replicate")
	}
	if lag := r.replicaLag(ctx, pol); lag != "" && lag != pol.Status.ReplicaLag {
		latest := &portagev1alpha1.Policy{}
		if err := r.Get(ctx, req.NamespacedName, latest); err == nil {
			latest.Status.ReplicaLag = lag
			_ = r.Status().Update(ctx, latest)
		}
	}
	requeue := 2 * time.Minute
	if pol.Spec.Replicate.Enabled {
		requeue = 30 * time.Second
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

func (r *PolicyReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *PolicyReconciler) loadPair(ctx context.Context, pol *portagev1alpha1.Policy) *portagev1alpha1.ClusterPair {
	if pol.Spec.ClusterPair == "" {
		return nil
	}
	pair := &portagev1alpha1.ClusterPair{}
	if err := r.Get(ctx, types.NamespacedName{Name: pol.Spec.ClusterPair}, pair); err != nil {
		return nil
	}
	return pair
}

// maybeBackup creates a Backup Action once per RPO window.
func (r *PolicyReconciler) maybeBackup(ctx context.Context, pol *portagev1alpha1.Policy) error {
	if !pol.Spec.Backup.Enabled || pol.Spec.Backup.RPO == "" {
		return nil
	}
	d, err := time.ParseDuration(pol.Spec.Backup.RPO)
	if err != nil || d <= 0 {
		return nil
	}
	name := rpoBackupName(pol.Name, r.now(), d)
	existing := &portagev1alpha1.Action{}
	err = r.Get(ctx, types.NamespacedName{Name: name, Namespace: pol.Namespace}, existing)
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}
	act := &portagev1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: pol.Namespace,
			Labels: map[string]string{
				"portage.io/rpo":    "true",
				"portage.io/policy": pol.Name,
			},
		},
		Spec: portagev1alpha1.ActionSpec{
			Type:      portagev1alpha1.ActionBackup,
			PolicyRef: pol.Name,
		},
	}
	return r.Create(ctx, act)
}

// maybeReplicate ensures one long-lived Replicate Action while
// spec.replicate.enabled. The Action stays CatchingUp and live-syncs dest.
func (r *PolicyReconciler) maybeReplicate(ctx context.Context, pol *portagev1alpha1.Policy) error {
	if !pol.Spec.Replicate.Enabled {
		return nil
	}
	name := replicateName(pol.Name)
	existing := &portagev1alpha1.Action{}
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: pol.Namespace}, existing)
	if err == nil {
		if existing.Spec.Type != portagev1alpha1.ActionReplicate {
			return nil
		}
		if existing.Labels == nil {
			existing.Labels = map[string]string{}
		}
		if existing.Labels["portage.io/live-replica"] != "true" {
			existing.Labels["portage.io/live-replica"] = "true"
			return r.Update(ctx, existing)
		}
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}
	act := &portagev1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: pol.Namespace,
			Labels: map[string]string{
				"portage.io/replicate":    "true",
				"portage.io/live-replica": "true",
				"portage.io/policy":       pol.Name,
			},
		},
		Spec: portagev1alpha1.ActionSpec{
			Type:      portagev1alpha1.ActionReplicate,
			PolicyRef: pol.Name,
		},
	}
	return r.Create(ctx, act)
}

func replicateName(policy string) string {
	n := "replicate-" + policy
	if len(n) > 63 {
		n = n[:63]
	}
	return n
}

func (r *PolicyReconciler) replicaLag(ctx context.Context, pol *portagev1alpha1.Policy) string {
	if !pol.Spec.Replicate.Enabled {
		return ""
	}
	act := &portagev1alpha1.Action{}
	if err := r.Get(ctx, types.NamespacedName{Name: replicateName(pol.Name), Namespace: pol.Namespace}, act); err != nil {
		return ""
	}
	return act.Status.Message
}

func rpoBackupName(policy string, now time.Time, d time.Duration) string {
	slot := now.UTC().Truncate(d).Unix()
	n := fmt.Sprintf("backup-%s-%d", policy, slot)
	if len(n) > 63 {
		n = n[:63]
	}
	return n
}

// maybeAutoRestore creates a Restore Action when Policy.spec.restore.auto is
// set, backups are useful, and a covered PVC is gone. Never overwrites a Bound PVC.
func (r *PolicyReconciler) maybeAutoRestore(ctx context.Context, pol *portagev1alpha1.Policy, inv classify.Inventory, dest kubernetes.Interface) error {
	if !pol.Spec.Restore.Auto {
		return nil
	}
	if !pol.Status.BackupHealthy {
		return nil
	}
	kube := dest
	if kube == nil {
		kube = r.KubeClient
	}
	need := false
	for _, w := range inv.Stateful() {
		for _, pvcName := range w.PVCNames {
			_, err := kube.CoreV1().PersistentVolumeClaims(w.Namespace).Get(ctx, pvcName, metav1.GetOptions{})
			if errors.IsNotFound(err) {
				need = true
				break
			}
		}
	}
	if !need {
		return nil
	}
	name := autoRestoreName(pol.Name)
	existing := &portagev1alpha1.Action{}
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: pol.Namespace}, existing)
	if err == nil {
		return nil // already running or done; do not stack
	}
	if !errors.IsNotFound(err) {
		return err
	}
	act := &portagev1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: pol.Namespace,
			Labels: map[string]string{
				"portage.io/auto":   "true",
				"portage.io/policy": pol.Name,
			},
		},
		Spec: portagev1alpha1.ActionSpec{
			Type:      portagev1alpha1.ActionRestore,
			PolicyRef: pol.Name,
		},
	}
	return r.Create(ctx, act)
}

func autoRestoreName(policy string) string {
	n := "restore-auto-" + policy
	if len(n) > 63 {
		n = n[:63]
	}
	return n
}

// SetupWithManager registers the reconciler.
func (r *PolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&portagev1alpha1.Policy{}).
		Complete(r)
}
