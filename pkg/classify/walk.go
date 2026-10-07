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

package classify

import (
	"context"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
)

// Namespaces lists namespaces to inventory. Empty means the given defaultNS.
func Namespaces(requested []string, defaultNS string) []string {
	if len(requested) == 0 {
		if defaultNS == "" {
			return []string{"default"}
		}
		return []string{defaultNS}
	}
	return requested
}

// Walk inventories StatefulSets, Deployments, DaemonSets and any leftover
// PVCs that no controller owns. Unknown + PVC becomes UnknownStateful.
func Walk(ctx context.Context, client kubernetes.Interface, namespaces []string) (Inventory, error) {
	inv := Inventory{Namespaces: append([]string(nil), namespaces...)}
	claimed := map[string]struct{}{} // ns/pvc claimed by a workload

	for _, ns := range namespaces {
		sts, err := client.AppsV1().StatefulSets(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return Inventory{}, fmt.Errorf("list StatefulSets in %s: %w", ns, err)
		}
		for i := range sts.Items {
			w := fromPodOwner(ns, "StatefulSet", "apps/v1", sts.Items[i].Name, sts.Items[i].Spec.Template.Spec, stsPVCs(&sts.Items[i]))
			inv.Workloads = append(inv.Workloads, w)
			markClaimed(claimed, ns, w.PVCNames)
		}

		deps, err := client.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return Inventory{}, fmt.Errorf("list Deployments in %s: %w", ns, err)
		}
		for i := range deps.Items {
			w := fromPodOwner(ns, "Deployment", "apps/v1", deps.Items[i].Name, deps.Items[i].Spec.Template.Spec, podPVCs(deps.Items[i].Spec.Template.Spec))
			inv.Workloads = append(inv.Workloads, w)
			markClaimed(claimed, ns, w.PVCNames)
		}

		dss, err := client.AppsV1().DaemonSets(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return Inventory{}, fmt.Errorf("list DaemonSets in %s: %w", ns, err)
		}
		for i := range dss.Items {
			w := fromPodOwner(ns, "DaemonSet", "apps/v1", dss.Items[i].Name, dss.Items[i].Spec.Template.Spec, podPVCs(dss.Items[i].Spec.Template.Spec))
			inv.Workloads = append(inv.Workloads, w)
			markClaimed(claimed, ns, w.PVCNames)
		}

		pvcs, err := client.CoreV1().PersistentVolumeClaims(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return Inventory{}, fmt.Errorf("list PVCs in %s: %w", ns, err)
		}
		for i := range pvcs.Items {
			key := ns + "/" + pvcs.Items[i].Name
			if _, ok := claimed[key]; ok {
				continue
			}
			if IsMoverScratchPVC(pvcs.Items[i]) {
				// VolSync cache/clone PVCs are not user data. Treating them as
				// workloads creates nested ReplicationSources (cache-of-cache)
				// that steal the restic lock and starve the real PVC.
				continue
			}
			inv.Workloads = append(inv.Workloads, Workload{
				Namespace:    ns,
				Name:         pvcs.Items[i].Name,
				Kind:         "PersistentVolumeClaim",
				APIVersion:   "v1",
				Class:        portagev1alpha1.ClassUnknownStateful,
				PVCNames:     []string{pvcs.Items[i].Name},
				Unclassified: true,
			})
		}
	}

	sort.Slice(inv.Workloads, func(i, j int) bool {
		if inv.Workloads[i].Namespace != inv.Workloads[j].Namespace {
			return inv.Workloads[i].Namespace < inv.Workloads[j].Namespace
		}
		if inv.Workloads[i].Kind != inv.Workloads[j].Kind {
			return inv.Workloads[i].Kind < inv.Workloads[j].Kind
		}
		return inv.Workloads[i].Name < inv.Workloads[j].Name
	})
	return inv, nil
}

func fromPodOwner(ns, kind, apiVersion, name string, spec corev1.PodSpec, pvcs []string) Workload {
	images := podImages(spec)
	w := Workload{
		Namespace:  ns,
		Name:       name,
		Kind:       kind,
		APIVersion: apiVersion,
		Images:     images,
		PVCNames:   pvcs,
		Class:      portagev1alpha1.ClassStateless,
	}
	if spec.SecurityContext != nil {
		w.FSGroup = spec.SecurityContext.FSGroup
		w.RunAsUser = spec.SecurityContext.RunAsUser
	}
	if eng, ok := matchImages(images); ok {
		w.Engine = eng.Name
		w.Class = eng.Class
		if len(pvcs) == 0 && w.Class != portagev1alpha1.ClassStateless {
			// Engine image without a disk: still treat as that class (emptyDir / operator volume).
		}
		return w
	}
	if len(pvcs) > 0 {
		w.Class = portagev1alpha1.ClassGenericPVC
	}
	return w
}

func matchImages(images []string) (Engine, bool) {
	for _, img := range images {
		if e, ok := MatchImage(img); ok {
			return e, true
		}
	}
	return Engine{}, false
}

func podImages(spec corev1.PodSpec) []string {
	var out []string
	for _, c := range spec.InitContainers {
		out = append(out, c.Image)
	}
	for _, c := range spec.Containers {
		out = append(out, c.Image)
	}
	return out
}

func podPVCs(spec corev1.PodSpec) []string {
	var names []string
	seen := map[string]struct{}{}
	for _, v := range spec.Volumes {
		if v.PersistentVolumeClaim == nil {
			continue
		}
		n := v.PersistentVolumeClaim.ClaimName
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		names = append(names, n)
	}
	return names
}

func stsPVCs(sts *appsv1.StatefulSet) []string {
	names := podPVCs(sts.Spec.Template.Spec)
	seen := map[string]struct{}{}
	for _, n := range names {
		seen[n] = struct{}{}
	}
	replicas := int32(1)
	if sts.Spec.Replicas != nil && *sts.Spec.Replicas > 0 {
		replicas = *sts.Spec.Replicas
	}
	for _, vct := range sts.Spec.VolumeClaimTemplates {
		// Realized claim is <template>-<sts>-<ordinal>. The bare template
		// name is not a PVC; VolSync destinationPVC of that name never binds.
		for i := int32(0); i < replicas; i++ {
			n := fmt.Sprintf("%s-%s-%d", vct.Name, sts.Name, i)
			if _, ok := seen[n]; ok {
				continue
			}
			seen[n] = struct{}{}
			names = append(names, n)
		}
	}
	return names
}

func markClaimed(claimed map[string]struct{}, ns string, pvcs []string) {
	for _, p := range pvcs {
		claimed[ns+"/"+p] = struct{}{}
	}
}

// ScratchPVCName is true for VolSync restic cache/clone claim names.
// Current movers use volsync-src-<owner>-cache / volsync-dst-<owner>-cache.
// Older VolSync used volsync-<owner>-cache (no src/dst infix), which is
// what dest K8up Schedules have been mounting.
func ScratchPVCName(name string) bool {
	if name == "" {
		return false
	}
	if strings.HasPrefix(name, "volsync-src-") || strings.HasPrefix(name, "volsync-dst-") {
		return true
	}
	return strings.HasPrefix(name, "volsync-") && strings.HasSuffix(name, "-cache")
}

// IsMoverScratchPVC is true for volumes VolSync (or a similar mover) created
// to hold restic/rclone cache or clones. They must not be inventoried.
func IsMoverScratchPVC(pvc corev1.PersistentVolumeClaim) bool {
	if pvc.Labels["app.kubernetes.io/created-by"] == "volsync" {
		return true
	}
	for _, o := range pvc.OwnerReferences {
		if strings.HasPrefix(o.APIVersion, "volsync.backube/") &&
			(o.Kind == "ReplicationSource" || o.Kind == "ReplicationDestination") {
			return true
		}
	}
	return ScratchPVCName(pvc.Name)
}
