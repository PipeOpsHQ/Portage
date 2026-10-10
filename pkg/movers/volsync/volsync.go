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

// Package volsync emits VolSync ReplicationSource/Destination CRs.
// Portage does not ship rsync; VolSync is the data plane.
package volsync

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/movers"
	"github.com/PipeOpsHQ/portage/pkg/objectstore"
)

var (
	srcGVR = schema.GroupVersionResource{Group: "volsync.backube", Version: "v1alpha1", Resource: "replicationsources"}
	dstGVR = schema.GroupVersionResource{Group: "volsync.backube", Version: "v1alpha1", Resource: "replicationdestinations"}
)

// Mover creates VolSync CRs. ObjectStore uses restic (incremental) unless
// ObjectMover=rclone. Direct uses rsyncTLS.
type Mover struct {
	Dynamic       dynamic.Interface // source cluster
	DestDynamic   dynamic.Interface // dest cluster; nil ⇒ dest is Dynamic (same cluster)
	Kube          kubernetes.Interface
	DestKube      kubernetes.Interface
	Transport     portagev1alpha1.TransportType
	DestPath      string // s3://bucket/prefix
	Schedule      string
	Creds         objectstore.Creds
	CopyMethod    string // Direct (kind) or Snapshot (CSI)
	SnapshotClass string
	// ObjectMover is "restic" (default ObjectStore, incremental) or "rclone".
	ObjectMover     string
	StorageClassMap map[string]string
}

func (m Mover) destDyn() dynamic.Interface {
	if m.DestDynamic != nil {
		return m.DestDynamic
	}
	if m.remoteDest() {
		return nil
	}
	return m.Dynamic
}

func (m Mover) destKube() kubernetes.Interface {
	if m.DestKube != nil {
		return m.DestKube
	}
	return m.Kube
}

func (m Mover) remoteDest() bool {
	return m.DestKube != nil && m.Kube != nil && m.DestKube != m.Kube
}

func (m Mover) Name() string {
	if m.ObjectMover == "rclone" {
		return "rclone"
	}
	return "volsync"
}

func (m Mover) Classes() []portagev1alpha1.WorkloadClass {
	return []portagev1alpha1.WorkloadClass{
		portagev1alpha1.ClassGenericPVC,
		portagev1alpha1.ClassUnknownStateful,
		portagev1alpha1.ClassSearchFS,
		portagev1alpha1.ClassQueueDurable,
		portagev1alpha1.ClassObjectStore,
		portagev1alpha1.ClassSQLLogical,
		portagev1alpha1.ClassKVLogical,
	}
}

func (m Mover) Discover(_ context.Context, w classify.Workload) (movers.Capability, error) {
	if len(w.PVCNames) == 0 {
		return movers.Capability{}, nil
	}
	return movers.Capability{Backup: true, Replicate: true, Restore: true}, nil
}

func (m Mover) Backup(ctx context.Context, w classify.Workload, _ movers.ClusterHandle) (movers.Artifact, error) {
	if m.Dynamic == nil {
		return movers.Artifact{Mover: m.Name(), Message: "volsync: dynamic client required"}, fmt.Errorf("volsync: dynamic client required")
	}
	pvc := firstDataPVC(w.PVCNames)
	if pvc == "" {
		return movers.Artifact{Mover: m.Name(), Message: "no pvc"}, nil
	}
	if err := EnsureSecrets(ctx, m.Kube, w.Namespace, m.Creds, m.objectPath(w)); err != nil {
		return movers.Artifact{Mover: m.Name()}, fmt.Errorf("volsync source secrets: %w", err)
	}
	if needsPrivilegedMover(w) {
		if err := enablePrivilegedMovers(ctx, m.Kube, w.Namespace); err != nil {
			return movers.Artifact{Mover: m.Name()}, fmt.Errorf("volsync privileged movers: %w", err)
		}
	}
	name := "portage-" + w.Name
	src := m.source(w, name, pvc)
	_, err := m.Dynamic.Resource(srcGVR).Namespace(w.Namespace).Create(ctx, src, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return movers.Artifact{Mover: m.Name()}, fmt.Errorf("volsync source: %w", err)
	}
	if err := m.protectCaches(ctx, w.Namespace); err != nil {
		return movers.Artifact{Mover: m.Name()}, fmt.Errorf("volsync scratch PVCs: %w", err)
	}
	return m.artifactFromSource(ctx, w, name)
}

func (m Mover) Replicate(ctx context.Context, w classify.Workload, _, _ movers.ClusterHandle) error {
	if m.Dynamic == nil {
		return fmt.Errorf("volsync: dynamic client required")
	}
	if len(w.PVCNames) == 0 {
		return nil
	}
	pvc := firstDataPVC(w.PVCNames)
	name := "portage-" + w.Name
	if pvc == "" {
		return m.protectCaches(ctx, w.Namespace)
	}
	// A deleted workload's PVC must not keep a ReplicationSource. Walk
	// still names a claim from a StatefulSet template until the claim is
	// gone; NotFound means there is nothing to sync.
	if missing, err := m.sourcePVCMissing(ctx, w.Namespace, pvc); err != nil {
		return err
	} else if missing {
		m.deletePair(ctx, w.Namespace, name)
		return nil
	}
	path := m.objectPath(w)
	if err := EnsureSecrets(ctx, m.Kube, w.Namespace, m.Creds, path); err != nil {
		return fmt.Errorf("volsync source secrets: %w", err)
	}
	if needsPrivilegedMover(w) {
		if err := enablePrivilegedMovers(ctx, m.Kube, w.Namespace); err != nil {
			return fmt.Errorf("volsync privileged movers: %w", err)
		}
	}
	if dk := m.destKube(); dk != nil && dk != m.Kube {
		if err := ensureNamespace(ctx, dk, w.Namespace); err != nil {
			return fmt.Errorf("volsync dest namespace: %w", err)
		}
		if needsPrivilegedMover(w) {
			if err := enablePrivilegedMovers(ctx, dk, w.Namespace); err != nil {
				return fmt.Errorf("volsync dest privileged movers: %w", err)
			}
		}
		if err := CopySecrets(ctx, m.Kube, dk, w.Namespace); err != nil {
			return fmt.Errorf("volsync dest secrets: %w", err)
		}
		for _, claim := range w.PVCNames {
			if isScratchPVC(claim) {
				continue
			}
			if err := m.ensureDestPVC(ctx, w.Namespace, claim); err != nil {
				return err
			}
		}
		m.stripDestScheduling(ctx, w.Namespace)
	}
	dstClient := m.destDyn()
	if dstClient == nil {
		return fmt.Errorf("volsync: dest dynamic client required")
	}
	if dstClient == m.Dynamic && m.remoteDest() {
		return fmt.Errorf("volsync: dest dynamic client is the source cluster; refusing to write ReplicationDestination in-cluster")
	}
	m.scrubMisplaced(ctx, w.Namespace, name)
	// Dest restic also runs `restic init` when the repository is empty.
	// Doing that at the same time as the source init overwrites config and
	// keys; every later mover then fails with "ciphertext verification failed".
	// Wait until the source backup has lastSyncTime, which means init and
	// the first snapshot finished.
	synced := true
	if m.resticCopy() {
		var err error
		synced, err = m.sourceHasSnapshot(ctx, w.Namespace, name)
		if err != nil {
			return err
		}
	}
	src := m.source(w, name, pvc)
	if err := applyNamespaced(ctx, m.Dynamic, srcGVR, src); err != nil {
		return fmt.Errorf("volsync source: %w", err)
	}
	if m.resticCopy() && !synced {
		return m.protectCaches(ctx, w.Namespace)
	}
	dst := m.destination(w, name)
	// A Running user pod already mounts this PVC (Restore brought Postgres
	// up). Direct copyMethod schedules the mover onto that same node, so
	// the next restic restore rewrites PGDATA, including postmaster.pid,
	// under the live postmaster. Pause and delete the in-flight mover.
	if destUserMounted(ctx, m.destKube(), w.Namespace, pvc) {
		_ = unstructured.SetNestedField(dst.Object, true, "spec", "paused")
		deleteWorkloadMoverJobs(ctx, m.destKube(), w.Namespace, name)
	}
	if err := applyNamespaced(ctx, dstClient, dstGVR, dst); err != nil {
		return fmt.Errorf("volsync destination: %w", err)
	}
	return m.protectCaches(ctx, w.Namespace)
}

// sourcePVCMissing is true when the source claim is gone. A nil kube client
// cannot check, so callers proceed (unit tests without a typed client).
func (m Mover) sourcePVCMissing(ctx context.Context, ns, pvc string) (bool, error) {
	if m.Kube == nil || ns == "" || pvc == "" {
		return false, nil
	}
	_, err := m.Kube.CoreV1().PersistentVolumeClaims(ns).Get(ctx, pvc, metav1.GetOptions{})
	if err == nil {
		return false, nil
	}
	if errors.IsNotFound(err) {
		return true, nil
	}
	return false, fmt.Errorf("volsync source pvc %s/%s: %w", ns, pvc, err)
}

// deletePair removes the Portage ReplicationSource and ReplicationDestination
// for one workload. Missing objects are ignored.
func (m Mover) deletePair(ctx context.Context, ns, name string) {
	if ns == "" || name == "" {
		return
	}
	if m.Dynamic != nil {
		_ = m.Dynamic.Resource(srcGVR).Namespace(ns).Delete(ctx, name, metav1.DeleteOptions{})
	}
	if dst := m.destDyn(); dst != nil {
		_ = dst.Resource(dstGVR).Namespace(ns).Delete(ctx, name, metav1.DeleteOptions{})
	}
}

// Prune deletes Portage VolSync pairs in ns that are not in keep (workload
// name → present) or whose source PVC no longer exists. CRs without
// portage.io/name are left alone.
func (m Mover) Prune(ctx context.Context, ns string, keep map[string]struct{}) error {
	if m.Dynamic == nil || ns == "" {
		return nil
	}
	list, err := m.Dynamic.Resource(srcGVR).Namespace(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for i := range list.Items {
		item := &list.Items[i]
		owner := item.GetLabels()["portage.io/name"]
		if owner == "" {
			continue
		}
		if _, ok := keep[owner]; ok {
			pvc, _, _ := unstructured.NestedString(item.Object, "spec", "sourcePVC")
			missing, err := m.sourcePVCMissing(ctx, ns, pvc)
			if err != nil {
				return err
			}
			if !missing {
				continue
			}
		}
		m.deletePair(ctx, ns, item.GetName())
	}
	return nil
}

// scrubMisplaced drops CRs that a bad reconcile (dest=source fallback or
// cluster-object copy) left on the wrong cluster. Those leftovers claim the
// cache PVC name and block the correct CR forever.
func (m Mover) scrubMisplaced(ctx context.Context, ns, name string) {
	if m.destDyn() == nil || m.destDyn() == m.Dynamic {
		return
	}
	_ = m.destDyn().Resource(srcGVR).Namespace(ns).Delete(ctx, name, metav1.DeleteOptions{})
	_ = m.Dynamic.Resource(dstGVR).Namespace(ns).Delete(ctx, name, metav1.DeleteOptions{})
}

func applyNamespaced(ctx context.Context, dyn dynamic.Interface, gvr schema.GroupVersionResource, obj *unstructured.Unstructured) error {
	if dyn == nil {
		return fmt.Errorf("client required")
	}
	ns, name := obj.GetNamespace(), obj.GetName()
	_, err := dyn.Resource(gvr).Namespace(ns).Create(ctx, obj, metav1.CreateOptions{})
	if err == nil || !errors.IsAlreadyExists(err) {
		return err
	}
	cur, err := dyn.Resource(gvr).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	obj.SetResourceVersion(cur.GetResourceVersion())
	obj.SetUID(cur.GetUID())
	// Status is a subresource. Keep it on the object so a fake client
	// matches the apiserver, which drops status from this update.
	if st, ok := cur.Object["status"]; ok {
		obj.Object["status"] = st
	}
	_, err = dyn.Resource(gvr).Namespace(ns).Update(ctx, obj, metav1.UpdateOptions{})
	return err
}

// resticCopy is the ObjectStore restic path. rclone and rsyncTLS do not
// init a shared encrypted repository.
func (m Mover) resticCopy() bool {
	return m.Transport == portagev1alpha1.TransportObjectStore && m.ObjectMover != "rclone"
}

// sourceHasSnapshot is true once ReplicationSource.status.lastSyncTime is set.
// NotFound is not an error: the source CR is created on this same call.
func (m Mover) sourceHasSnapshot(ctx context.Context, ns, name string) (bool, error) {
	if m.Dynamic == nil || ns == "" || name == "" {
		return false, nil
	}
	obj, err := m.Dynamic.Resource(srcGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("volsync source sync: %w", err)
	}
	s, found, _ := unstructured.NestedString(obj.Object, "status", "lastSyncTime")
	return found && s != "", nil
}

func (m Mover) Restore(context.Context, classify.Workload, movers.Artifact, movers.ClusterHandle) error {
	return nil
}
func (m Mover) Quiesce(context.Context, classify.Workload) error { return nil }
func (m Mover) Promote(context.Context, classify.Workload, movers.ClusterHandle) error {
	return nil
}
func (m Mover) Probe(ctx context.Context, w classify.Workload, _ movers.ClusterHandle) (movers.ProbeResult, error) {
	if pin := m.destMoverPinMessage(ctx, w.Namespace); pin != "" {
		return movers.ProbeResult{OK: false, Message: pin}, nil
	}
	ok, err := m.LagZero(ctx, w)
	if err != nil {
		return movers.ProbeResult{OK: false, Message: err.Error()}, err
	}
	if !ok {
		return movers.ProbeResult{OK: false, Message: "volsync lastSyncTime not set on source and dest"}, nil
	}
	return movers.ProbeResult{OK: true, Message: "volsync lastSyncTime set on source and dest"}, nil
}

// LagZero is true when both ReplicationSource and ReplicationDestination have lastSyncTime.
func (m Mover) LagZero(ctx context.Context, w classify.Workload) (bool, error) {
	srcOK := synced(ctx, m.Dynamic, srcGVR, w.Namespace, "portage-"+w.Name)
	dstOK := synced(ctx, m.destDyn(), dstGVR, w.Namespace, "portage-"+w.Name)
	return srcOK && dstOK, nil
}

func synced(ctx context.Context, dyn dynamic.Interface, gvr schema.GroupVersionResource, ns, name string) bool {
	if dyn == nil {
		return false
	}
	obj, err := dyn.Resource(gvr).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return false
	}
	if s, found, _ := unstructured.NestedString(obj.Object, "status", "lastSyncTime"); found && s != "" {
		return true
	}
	// ReplicationDestination also reports latestImage once a restore snapshot exists.
	if _, found, _ := unstructured.NestedFieldNoCopy(obj.Object, "status", "latestImage"); found {
		return true
	}
	return false
}

func (m Mover) source(w classify.Workload, name, pvc string) *unstructured.Unstructured {
	spec := map[string]any{
		"sourcePVC": pvc,
		"trigger":   m.trigger(),
	}
	if m.Transport == portagev1alpha1.TransportObjectStore {
		if m.ObjectMover == "rclone" {
			spec["rclone"] = m.rcloneSpec(w)
		} else {
			spec["restic"] = m.resticSpec(w)
		}
	} else {
		spec["rsyncTLS"] = map[string]any{
			"copyMethod": m.copyMethod(),
			"keySecret":  tlsSecretName,
		}
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "volsync.backube/v1alpha1",
		"kind":       "ReplicationSource",
		"metadata": map[string]any{
			"name":      name,
			"namespace": w.Namespace,
			"labels":    map[string]any{"portage.io/name": w.Name},
		},
		"spec": spec,
	}}
}

func (m Mover) destination(w classify.Workload, name string) *unstructured.Unstructured {
	spec := map[string]any{}
	if m.Transport == portagev1alpha1.TransportObjectStore {
		// Dest must pull on a schedule. A one-shot manual trigger is the
		// live-sync hole: source keeps snapshotting, dest never applies.
		spec["trigger"] = m.trigger()
		if m.ObjectMover == "rclone" {
			rc := m.rcloneSpec(w)
			stripDestMoverScheduling(rc)
			spec["rclone"] = rc
		} else {
			spec["restic"] = m.destResticSpec(w)
		}
	} else {
		spec["rsyncTLS"] = map[string]any{
			"serviceType": "LoadBalancer",
			"keySecret":   tlsSecretName,
		}
		stripDestMoverScheduling(spec["rsyncTLS"].(map[string]any))
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "volsync.backube/v1alpha1",
		"kind":       "ReplicationDestination",
		"metadata": map[string]any{
			"name":      name,
			"namespace": w.Namespace,
			"labels":    map[string]any{"portage.io/name": w.Name},
		},
		"spec": spec,
	}}
}

func (m Mover) resticSpec(w classify.Workload) map[string]any {
	spec := map[string]any{
		"repository":        resticSecretName,
		"copyMethod":        m.copyMethod(),
		"cacheCapacity":     "1Gi",
		"pruneIntervalDays": int64(7),
		"retain":            map[string]any{"hourly": int64(3), "daily": int64(1)},
	}
	if m.copyMethod() == "Snapshot" && m.SnapshotClass != "" {
		spec["volumeSnapshotClassName"] = m.SnapshotClass
	}
	if sc := moverSecurityContext(w); sc != nil {
		spec["moverSecurityContext"] = sc
	}
	return spec
}

// destResticSpec is the restore-side restic block. pruneIntervalDays/retain
// exist only on ReplicationSource; putting them on dest is a field-validation
// warning and does not restore bytes. destinationPVC is the realized claim
// name; Replicate creates that PVC on dest before this CR is applied.
func (m Mover) destResticSpec(w classify.Workload) map[string]any {
	spec := map[string]any{
		"repository":    resticSecretName,
		"copyMethod":    m.copyMethod(),
		"cacheCapacity": "1Gi",
	}
	if pvc := firstDataPVC(w.PVCNames); pvc != "" {
		spec["destinationPVC"] = pvc
	} else {
		spec["accessModes"] = []any{"ReadWriteOnce"}
		spec["capacity"] = "1Gi"
	}
	if m.copyMethod() == "Snapshot" && m.SnapshotClass != "" {
		spec["volumeSnapshotClassName"] = m.SnapshotClass
	}
	if sc := destMoverSecurityContext(w); sc != nil {
		spec["moverSecurityContext"] = sc
	}
	stripDestMoverScheduling(spec)
	return spec
}

func moverSecurityContext(w classify.Workload) map[string]any {
	if w.FSGroup == nil && w.RunAsUser == nil {
		return nil
	}
	sc := map[string]any{}
	if w.FSGroup != nil {
		sc["fsGroup"] = *w.FSGroup
	}
	if w.RunAsUser != nil {
		sc["runAsUser"] = *w.RunAsUser
	}
	return sc
}

// destMoverSecurityContext sets runAsUser and never fsGroup. Any fsGroup
// makes kubelet OR 0660 onto the volume at mount, before restic writes and
// again when the dest Postgres pod mounts. server.key must stay 0600.
// Privileged movers (fsGroup unset or 0) restore as root and keep the
// archived mode. runAsUser is the engine UID so a non-privileged mover
// still matches the data owner.
func destMoverSecurityContext(w classify.Workload) map[string]any {
	var uid int64
	if w.RunAsUser != nil && *w.RunAsUser != 0 {
		uid = *w.RunAsUser
	} else {
		uid = engineRestoreUID(w)
	}
	if uid == 0 {
		return nil
	}
	return map[string]any{"runAsUser": uid}
}

func engineRestoreUID(w classify.Workload) int64 {
	switch w.Engine {
	case "mysql", "mariadb":
		return 27
	case "postgres", "redis", "mongo", "mongodb":
		return 999
	}
	switch w.Class {
	case portagev1alpha1.ClassSQLLogical, portagev1alpha1.ClassKVLogical:
		return 999
	}
	return 0
}

// VolSync grants DAC_OVERRIDE (and CHOWN/FOWNER) only when the namespace
// is annotated. Marketplace images that own PVC data (mode 700, UID 999)
// via capabilities instead of declaring fsGroup need that fallback.
const privilegedMoversAnnotation = "volsync.backube/privileged-movers"

func needsPrivilegedMover(w classify.Workload) bool {
	if w.RunAsUser != nil && *w.RunAsUser != 0 {
		return false
	}
	if w.FSGroup != nil && *w.FSGroup != 0 {
		return false
	}
	return true
}

func enablePrivilegedMovers(ctx context.Context, kube kubernetes.Interface, ns string) error {
	if kube == nil || ns == "" {
		return nil
	}
	cur, err := kube.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if cur.Annotations != nil && cur.Annotations[privilegedMoversAnnotation] == "true" {
		return nil
	}
	if cur.Annotations == nil {
		cur.Annotations = map[string]string{}
	}
	cur.Annotations[privilegedMoversAnnotation] = "true"
	_, err = kube.CoreV1().Namespaces().Update(ctx, cur, metav1.UpdateOptions{})
	return err
}

// stripDestMoverScheduling clears dest-side VolSync mover tolerations.
// Direct copyMethod copies the mounting pod's tolerations onto the mover
// after it resolves spec.nodeName. Source RuntimeClass tolerations then
// fail dest scheduling. NodeSelector is not a CR field VolSync will clear;
// stripDestScheduling deletes Jobs and pods that still carry a foreign pin.
func stripDestMoverScheduling(spec map[string]any) {
	if spec == nil {
		return
	}
	spec["moverTolerations"] = []any{}
}

func (m Mover) rcloneSpec(w classify.Workload) map[string]any {
	return map[string]any{
		"rcloneConfigSection": "rclone",
		"rcloneConfig":        rcloneSecretName,
		"rcloneDestPath":      m.objectPath(w),
		"copyMethod":          m.copyMethod(),
	}
}

func (m Mover) objectPath(w classify.Workload) string {
	if m.DestPath != "" {
		return m.DestPath
	}
	return "s3://portage/" + w.Namespace + "/" + w.Name
}

func (m Mover) copyMethod() string {
	if m.CopyMethod != "" {
		return m.CopyMethod
	}
	if m.SnapshotClass != "" {
		return "Snapshot"
	}
	// Direct works on kind local-path (no CSI snapshots). Snapshot when a
	// VolumeSnapshotClass is mapped on the ClusterPair.
	return "Direct"
}

func (m Mover) trigger() map[string]any {
	// Schedule only. VolSync's initial state always starts a sync immediately;
	// after that the cron fires incrementals. Setting trigger.manual alongside
	// schedule makes VolSync treat the CR as manual-only, so once lastManualSync
	// matches, incrementals never run.
	return map[string]any{"schedule": m.schedule()}
}

func (m Mover) schedule() string {
	if m.Schedule != "" {
		return m.Schedule
	}
	return "*/5 * * * *"
}

func isScratchPVC(name string) bool {
	return classify.ScratchPVCName(name)
}

func firstDataPVC(names []string) string {
	for _, n := range names {
		if n != "" && !isScratchPVC(n) {
			return n
		}
	}
	return ""
}
