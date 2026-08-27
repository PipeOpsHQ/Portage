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
	ObjectMover string
}

func (m Mover) destDyn() dynamic.Interface {
	if m.DestDynamic != nil {
		return m.DestDynamic
	}
	return m.Dynamic
}

func (m Mover) destKube() kubernetes.Interface {
	if m.DestKube != nil {
		return m.DestKube
	}
	return m.Kube
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
	return movers.Artifact{Mover: m.Name(), Message: "volsync is replicate, not backup"}, nil
}

func (m Mover) Replicate(ctx context.Context, w classify.Workload, _, _ movers.ClusterHandle) error {
	if m.Dynamic == nil {
		return fmt.Errorf("volsync: dynamic client required")
	}
	if len(w.PVCNames) == 0 {
		return nil
	}
	path := m.objectPath(w)
	if err := EnsureSecrets(ctx, m.Kube, w.Namespace, m.Creds, path); err != nil {
		return fmt.Errorf("volsync source secrets: %w", err)
	}
	if dk := m.destKube(); dk != nil && dk != m.Kube {
		if err := EnsureSecrets(ctx, dk, w.Namespace, m.Creds, path); err != nil {
			return fmt.Errorf("volsync dest secrets: %w", err)
		}
	}
	pvc := w.PVCNames[0]
	name := "portage-" + w.Name
	src := m.source(w, name, pvc)
	_, err := m.Dynamic.Resource(srcGVR).Namespace(w.Namespace).Create(ctx, src, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("volsync source: %w", err)
	}
	dstClient := m.destDyn()
	if dstClient == nil {
		return fmt.Errorf("volsync: dest dynamic client required")
	}
	dst := m.destination(w, name)
	_, err = dstClient.Resource(dstGVR).Namespace(w.Namespace).Create(ctx, dst, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("volsync destination: %w", err)
	}
	return nil
}

func (m Mover) Restore(context.Context, classify.Workload, movers.Artifact, movers.ClusterHandle) error {
	return nil
}
func (m Mover) Quiesce(context.Context, classify.Workload) error { return nil }
func (m Mover) Promote(context.Context, classify.Workload, movers.ClusterHandle) error {
	return nil
}
func (m Mover) Probe(ctx context.Context, w classify.Workload, _ movers.ClusterHandle) (movers.ProbeResult, error) {
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
		"trigger":   map[string]any{"schedule": m.schedule()},
	}
	if m.Transport == portagev1alpha1.TransportObjectStore {
		if m.ObjectMover == "rclone" {
			spec["rclone"] = m.rcloneSpec(w)
		} else {
			spec["restic"] = m.resticSpec()
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
		spec["trigger"] = map[string]any{"schedule": m.schedule()}
		if m.ObjectMover == "rclone" {
			spec["rclone"] = m.rcloneSpec(w)
		} else {
			rs := m.resticSpec()
			if len(w.PVCNames) > 0 {
				rs["destinationPVC"] = w.PVCNames[0]
			}
			rs["accessModes"] = []any{"ReadWriteOnce"}
			rs["capacity"] = "1Gi"
			spec["restic"] = rs
		}
	} else {
		spec["rsyncTLS"] = map[string]any{
			"serviceType": "LoadBalancer",
			"keySecret":   tlsSecretName,
		}
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

func (m Mover) resticSpec() map[string]any {
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
	return spec
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

func (m Mover) schedule() string {
	if m.Schedule != "" {
		return m.Schedule
	}
	return "*/5 * * * *"
}
