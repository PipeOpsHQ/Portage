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

package volsync

import (
	"context"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/PipeOpsHQ/portage/pkg/classify"
)

const (
	k8upBackupAnn     = "k8up.io/backup"
	k8upLegacyAnn     = "k8up.synextreme.com/backup"
	k8upAppuioAnn     = "backup.appuio.io/backup"
	stashSkipAnn      = "stash.appscode.com/skip"
	veleroExcludeLbl  = "velero.io/exclude-from-backup"
	portageScratchLbl = "portage.io/scratch"
)

func (m Mover) protectCaches(ctx context.Context, ns string) error {
	if err := ProtectScratchPVCs(ctx, m.Kube, ns); err != nil {
		return err
	}
	if dk := m.destKube(); dk != nil && dk != m.Kube {
		return ProtectScratchPVCs(ctx, dk, ns)
	}
	return nil
}

// ProtectScratchPVCs marks VolSync restic cache/clone claims so dest-local
// filesystem backup tools (K8up, Velero, Stash) skip them, then deletes
// backup Jobs already attached. Those volumes are RWO mover scratch; a
// K8up job that holds them blocks the dest ReplicationDestination mover
// (Multi-Attach) and leaves Replicate CatchingUp.
func ProtectScratchPVCs(ctx context.Context, kube kubernetes.Interface, ns string) error {
	if kube == nil || ns == "" {
		return nil
	}
	list, err := kube.CoreV1().PersistentVolumeClaims(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list PVCs in %s: %w", ns, err)
	}
	scratch := map[string]struct{}{}
	var errs []error
	for i := range list.Items {
		pvc := &list.Items[i]
		if !classify.IsMoverScratchPVC(*pvc) {
			continue
		}
		scratch[pvc.Name] = struct{}{}
		if err := markScratchPVC(ctx, kube, pvc); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("mark scratch PVC %s/%s: %w", ns, pvc.Name, err))
		}
	}
	if len(scratch) > 0 {
		evictBackupPodsFromScratch(ctx, kube, ns, scratch)
	}
	return errors.Join(errs...)
}

func markScratchPVC(ctx context.Context, kube kubernetes.Interface, pvc *corev1.PersistentVolumeClaim) error {
	cur, err := kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if !ensureScratchMarks(cur) {
		return nil
	}
	_, err = kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, cur, metav1.UpdateOptions{})
	return err
}

func ensureScratchMarks(pvc *corev1.PersistentVolumeClaim) bool {
	changed := false
	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}
	if pvc.Labels == nil {
		pvc.Labels = map[string]string{}
	}
	for k, v := range map[string]string{
		k8upBackupAnn: "false",
		k8upLegacyAnn: "false",
		k8upAppuioAnn: "false",
		stashSkipAnn:  "true",
	} {
		if pvc.Annotations[k] != v {
			pvc.Annotations[k] = v
			changed = true
		}
	}
	for k, v := range map[string]string{
		veleroExcludeLbl:  "true",
		portageScratchLbl: "true",
	} {
		if pvc.Labels[k] != v {
			pvc.Labels[k] = v
			changed = true
		}
	}
	return changed
}

func evictBackupPodsFromScratch(ctx context.Context, kube kubernetes.Interface, ns string, scratch map[string]struct{}) {
	pods, err := kube.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}
	jobs := map[string]struct{}{}
	bg := metav1.DeletePropagationBackground
	for i := range pods.Items {
		p := &pods.Items[i]
		if !podMountsScratch(p, scratch) || !isBackupToolPod(p) {
			continue
		}
		if job := jobOwner(p); job != "" {
			jobs[job] = struct{}{}
			continue
		}
		_ = kube.CoreV1().Pods(ns).Delete(ctx, p.Name, metav1.DeleteOptions{PropagationPolicy: &bg})
	}
	for job := range jobs {
		_ = kube.BatchV1().Jobs(ns).Delete(ctx, job, metav1.DeleteOptions{PropagationPolicy: &bg})
	}
}

func podMountsScratch(pod *corev1.Pod, scratch map[string]struct{}) bool {
	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim == nil {
			continue
		}
		if _, ok := scratch[v.PersistentVolumeClaim.ClaimName]; ok {
			return true
		}
	}
	return false
}

func jobOwner(pod *corev1.Pod) string {
	for _, o := range pod.OwnerReferences {
		if o.Kind == "Job" && o.Name != "" {
			return o.Name
		}
	}
	return ""
}

func isBackupToolPod(pod *corev1.Pod) bool {
	n := pod.Name
	if strings.HasPrefix(n, "volsync-src-") || strings.HasPrefix(n, "volsync-dst-") {
		return false
	}
	l := pod.Labels
	if l["k8up.io/type"] != "" || l["k8upjob"] != "" {
		return true
	}
	switch l["app.kubernetes.io/managed-by"] {
	case "k8up", "velero", "stash":
		return true
	}
	if l["velero.io/backup-name"] != "" || l["velero.io/restore-name"] != "" {
		return true
	}
	if strings.HasPrefix(n, "backup-") {
		return true
	}
	for _, c := range append(append([]corev1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...) {
		img := strings.ToLower(c.Image)
		if strings.Contains(img, "k8up") || strings.Contains(img, "velero") || strings.Contains(img, "stash") {
			return true
		}
	}
	return false
}
