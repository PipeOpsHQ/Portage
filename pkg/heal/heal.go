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

// Package heal applies bounded, named fixes so restored workloads can
// schedule. It does not retry empty data (that is a failed Action).
package heal

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/PipeOpsHQ/portage/pkg/transform"
)

const (
	StripSelectedNode = "strip-selected-node"
	RemapStorageClass = "remap-storageclass"
	StripNodeAffinity = "strip-node-affinity"
	StripZoneLabels   = "strip-zone-labels"
	BindPVCByName     = "bind-pvc-by-name"
)

// PVC strips cluster-local pins and remaps StorageClass. Returns healer names applied.
func PVC(pvc *corev1.PersistentVolumeClaim, opt transform.Options) []string {
	if pvc == nil {
		return nil
	}
	var out []string
	if pvc.Annotations != nil {
		if _, ok := pvc.Annotations["volume.kubernetes.io/selected-node"]; ok {
			out = append(out, StripSelectedNode)
		}
	}
	if pvc.Labels != nil {
		if _, ok := pvc.Labels["topology.kubernetes.io/zone"]; ok {
			out = append(out, StripZoneLabels)
		}
	}
	oldSC := ""
	if pvc.Spec.StorageClassName != nil {
		oldSC = *pvc.Spec.StorageClassName
	}
	if pvc.Status.Phase == corev1.ClaimBound {
		// Bound dest claims (VolSync) cannot have spec.volumeName cleared.
		if pvc.Annotations != nil {
			delete(pvc.Annotations, "volume.kubernetes.io/selected-node")
		}
		if pvc.Labels != nil {
			delete(pvc.Labels, "topology.kubernetes.io/zone")
		}
		return unique(out)
	}
	phase := pvc.Status.Phase
	transform.PVC(pvc, opt)
	pvc.Status.Phase = phase
	if pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != oldSC && oldSC != "" {
		out = append(out, RemapStorageClass)
	}
	if pvc.Spec.VolumeName != "" {
		// transform.PVC already clears this; record if we would have.
	}
	return unique(out)
}

// PodTemplate drops node pins that keep dest pods Pending.
func PodSpec(spec *corev1.PodSpec) []string {
	if spec == nil {
		return nil
	}
	var out []string
	if len(spec.NodeSelector) > 0 {
		spec.NodeSelector = nil
		out = append(out, StripNodeAffinity)
	}
	if spec.NodeName != "" {
		spec.NodeName = ""
		out = append(out, StripNodeAffinity)
	}
	if spec.Affinity != nil && spec.Affinity.NodeAffinity != nil {
		spec.Affinity.NodeAffinity = nil
		out = append(out, StripNodeAffinity)
	}
	spec.TopologySpreadConstraints = nil
	if n := filterPodTolerations(spec); n > 0 {
		out = append(out, StripNodeAffinity)
	}
	return unique(out)
}

func filterPodTolerations(spec *corev1.PodSpec) int {
	if spec == nil || len(spec.Tolerations) == 0 {
		return 0
	}
	kept := spec.Tolerations[:0]
	dropped := 0
	for _, t := range spec.Tolerations {
		if clusterLocalTol(t.Key) {
			dropped++
			continue
		}
		kept = append(kept, t)
	}
	spec.Tolerations = kept
	return dropped
}

func clusterLocalTol(key string) bool {
	switch key {
	case "sandbox.gke.io/runtime", "nvidia.com/gpu", "kubernetes.io/hostname":
		return true
	}
	return false
}

func unique(in []string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
