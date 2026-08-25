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

package snapshots

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"

	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/heal"
	"github.com/PipeOpsHQ/portage/pkg/transform"
)

// RehydrateOptions control dest PVC creation.
type RehydrateOptions struct {
	StorageClass   string
	NeverOverwrite bool
	Transform      transform.Options
	DefaultSize    string // e.g. "10Gi"
}

// EnsurePVCFromSnapshot binds dest PVC by the original name.
// If the claim is missing, it is created with dataSource=VolumeSnapshot.
// Existing Bound claims are left alone when NeverOverwrite is set.
func (c Client) EnsurePVCFromSnapshot(ctx context.Context, kube kubernetes.Interface, w classify.Workload, pvcName string, opt RehydrateOptions) (done bool, healed []string, err error) {
	if kube == nil || pvcName == "" {
		return false, nil, fmt.Errorf("rehydrate: kube client and pvc name required")
	}
	pvc, err := kube.CoreV1().PersistentVolumeClaims(w.Namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err == nil {
		healed = heal.PVC(pvc, opt.Transform)
		if len(healed) > 0 {
			if _, uerr := kube.CoreV1().PersistentVolumeClaims(w.Namespace).Update(ctx, pvc, metav1.UpdateOptions{}); uerr != nil {
				return false, healed, uerr
			}
		}
		if pvc.Status.Phase == corev1.ClaimBound && opt.NeverOverwrite {
			return true, healed, nil
		}
		if pvc.Status.Phase == corev1.ClaimBound || pvc.Status.Phase == corev1.ClaimPending {
			return pvc.Status.Phase == corev1.ClaimBound, healed, nil
		}
		return false, healed, nil
	}
	if !errors.IsNotFound(err) {
		return false, nil, err
	}

	ready, snapName, err := c.LatestReady(ctx, w)
	if err != nil {
		return false, nil, err
	}
	if !ready || snapName == "" {
		return false, nil, fmt.Errorf("no ReadyToUse VolumeSnapshot for %s pvc %s", w.Key(), pvcName)
	}

	size := opt.DefaultSize
	if size == "" {
		size = "10Gi"
	}
	if restore := c.restoreSize(ctx, w.Namespace, snapName); restore != "" {
		size = restore
	}
	sc := opt.StorageClass
	if sc == "" {
		sc = opt.Transform.DefaultStorageClass
	}

	created := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: w.Namespace,
			Labels: map[string]string{
				labelName:      w.Name,
				labelNamespace: w.Namespace,
				labelKind:      w.Kind,
				labelPVC:       pvcName,
			},
			Annotations: map[string]string{
				heal.BindPVCByName: snapName,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(size),
				},
			},
			DataSource: &corev1.TypedLocalObjectReference{
				APIGroup: strPtr("snapshot.storage.k8s.io"),
				Kind:     "VolumeSnapshot",
				Name:     snapName,
			},
		},
	}
	if sc != "" {
		created.Spec.StorageClassName = &sc
	}
	transform.PVC(created, opt.Transform)
	// transform clears DataSource — put it back. That is the restore handle.
	created.Spec.DataSource = &corev1.TypedLocalObjectReference{
		APIGroup: strPtr("snapshot.storage.k8s.io"),
		Kind:     "VolumeSnapshot",
		Name:     snapName,
	}
	if _, err := kube.CoreV1().PersistentVolumeClaims(w.Namespace).Create(ctx, created, metav1.CreateOptions{}); err != nil {
		if errors.IsAlreadyExists(err) {
			return false, []string{heal.BindPVCByName}, nil
		}
		return false, nil, err
	}
	return false, []string{heal.BindPVCByName}, nil
}

func (c Client) restoreSize(ctx context.Context, ns, name string) string {
	if c.Dynamic == nil {
		return ""
	}
	obj, err := c.Dynamic.Resource(snapshotGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	s, found, _ := unstructured.NestedString(obj.Object, "status", "restoreSize")
	if found {
		return s
	}
	return ""
}

func strPtr(s string) *string { return &s }
