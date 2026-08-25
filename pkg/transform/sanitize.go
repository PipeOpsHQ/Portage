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

// Package transform strips cluster-local fields so a dest cluster can schedule
// restored objects. This is the default Renderer=Sanitize path and defense in
// depth even when a PaaS re-renders desired state from scratch.
package transform

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Annotation keys that pin a workload to the source cloud or node.
var dropAnnotationExact = map[string]struct{}{
	"volume.kubernetes.io/selected-node":            {},
	"pv.kubernetes.io/bind-completed":               {},
	"pv.kubernetes.io/bound-by-controller":          {},
	"volume.beta.kubernetes.io/storage-provisioner": {},
	"volume.kubernetes.io/storage-provisioner":      {},
	"velero.io/backup-name":                         {},
	"velero.io/restore-name":                        {},
}

var dropAnnotationPrefixes = []string{
	"service.beta.kubernetes.io/aws-load-balancer-",
	"service.beta.kubernetes.io/azure-",
	"cloud.google.com/",
	"kubernetes.digitalocean.com/",
	"ovhcloud.com/",
	"metallb.universe.tf/",
}

var dropLabelKeys = []string{
	"topology.kubernetes.io/zone",
	"topology.kubernetes.io/region",
	"failure-domain.beta.kubernetes.io/zone",
	"failure-domain.beta.kubernetes.io/region",
	"topology.ebs.csi.aws.com/zone",
	"topology.gke.io/zone",
}

// Options controls sanitization.
type Options struct {
	// StorageClassMap remaps spec.storageClassName.
	StorageClassMap map[string]string
	// DefaultStorageClass is used when the source class is missing from the map.
	DefaultStorageClass string
}

// Object mutates a generic object: drop status, UID, resourceVersion,
// cluster-local labels/annotations.
func Object(obj *unstructured.Unstructured, opt Options) {
	if obj == nil {
		return
	}
	obj.SetUID("")
	obj.SetResourceVersion("")
	obj.SetGeneration(0)
	obj.SetManagedFields(nil)
	obj.SetOwnerReferences(nil)
	unstructured.RemoveNestedField(obj.Object, "status")
	unstructured.RemoveNestedField(obj.Object, "spec", "volumeName")
	unstructured.RemoveNestedField(obj.Object, "spec", "clusterIP")
	unstructured.RemoveNestedField(obj.Object, "spec", "clusterIPs")
	unstructured.RemoveNestedField(obj.Object, "spec", "nodeName")

	obj.SetAnnotations(filterAnns(obj.GetAnnotations()))
	obj.SetLabels(filterLabels(obj.GetLabels()))

	remapStorageClass(obj, opt)
	stripAffinity(obj)
}

// PVC applies Object plus claim-specific cleanup.
func PVC(pvc *corev1.PersistentVolumeClaim, opt Options) {
	if pvc == nil {
		return
	}
	pvc.Status = corev1.PersistentVolumeClaimStatus{}
	pvc.UID = ""
	pvc.ResourceVersion = ""
	pvc.Generation = 0
	pvc.ManagedFields = nil
	pvc.OwnerReferences = nil
	pvc.Annotations = filterAnns(pvc.Annotations)
	pvc.Labels = filterLabels(pvc.Labels)
	pvc.Spec.VolumeName = ""
	if pvc.Spec.StorageClassName != nil {
		mapped := mapSC(*pvc.Spec.StorageClassName, opt)
		pvc.Spec.StorageClassName = &mapped
	} else if opt.DefaultStorageClass != "" {
		sc := opt.DefaultStorageClass
		pvc.Spec.StorageClassName = &sc
	}
	if pvc.Spec.Selector != nil {
		pvc.Spec.Selector.MatchLabels = filterLabels(pvc.Spec.Selector.MatchLabels)
	}
	pvc.Spec.DataSource = nil
	pvc.Spec.DataSourceRef = nil
}

func filterAnns(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		if _, drop := dropAnnotationExact[k]; drop {
			continue
		}
		if hasPrefix(k, dropAnnotationPrefixes) {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func filterLabels(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		if contains(dropLabelKeys, k) {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func remapStorageClass(obj *unstructured.Unstructured, opt Options) {
	sc, found, _ := unstructured.NestedString(obj.Object, "spec", "storageClassName")
	if !found || sc == "" {
		if opt.DefaultStorageClass != "" {
			_ = unstructured.SetNestedField(obj.Object, opt.DefaultStorageClass, "spec", "storageClassName")
		}
		return
	}
	_ = unstructured.SetNestedField(obj.Object, mapSC(sc, opt), "spec", "storageClassName")
}

func mapSC(src string, opt Options) string {
	if opt.StorageClassMap != nil {
		if dst, ok := opt.StorageClassMap[src]; ok {
			return dst
		}
	}
	if opt.DefaultStorageClass != "" {
		return opt.DefaultStorageClass
	}
	return src
}

func stripAffinity(obj *unstructured.Unstructured) {
	unstructured.RemoveNestedField(obj.Object, "spec", "nodeSelector")
	unstructured.RemoveNestedField(obj.Object, "spec", "affinity", "nodeAffinity")
	unstructured.RemoveNestedField(obj.Object, "spec", "template", "spec", "nodeSelector")
	unstructured.RemoveNestedField(obj.Object, "spec", "template", "spec", "affinity", "nodeAffinity")
	unstructured.RemoveNestedField(obj.Object, "spec", "template", "spec", "topologySpreadConstraints")
}

func hasPrefix(k string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(k, p) {
			return true
		}
	}
	return false
}

func contains(list []string, k string) bool {
	for _, x := range list {
		if x == k {
			return true
		}
	}
	return false
}

// DropMetaForCreate is a helper so tests can assert annotations after Object().
func DropMetaForCreate(meta *metav1.ObjectMeta) {
	meta.UID = ""
	meta.ResourceVersion = ""
	meta.Generation = 0
	meta.ManagedFields = nil
	meta.Annotations = filterAnns(meta.Annotations)
	meta.Labels = filterLabels(meta.Labels)
}
