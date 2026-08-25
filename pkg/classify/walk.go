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
	for _, vct := range sts.Spec.VolumeClaimTemplates {
		// VCT materializes as <template>-<sts>-<ordinal>. Record the template
		// name; restore binds by realized claim name later.
		if _, ok := seen[vct.Name]; ok {
			continue
		}
		seen[vct.Name] = struct{}{}
		names = append(names, vct.Name)
	}
	return names
}

func markClaimed(claimed map[string]struct{}, ns string, pvcs []string) {
	for _, p := range pvcs {
		claimed[ns+"/"+p] = struct{}{}
	}
}
