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

package heal

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/PipeOpsHQ/portage/pkg/transform"
)

func TestPVCStripsSelectedNodeAndRemapsSC(t *testing.T) {
	t.Parallel()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data-pg",
			Annotations: map[string]string{
				"volume.kubernetes.io/selected-node": "gke-node-1",
			},
			Labels: map[string]string{"topology.kubernetes.io/zone": "b"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: ptr.To("premium-rwo"),
			VolumeName:       "pvc-deadbeef",
		},
	}
	got := PVC(pvc, transform.Options{StorageClassMap: map[string]string{"premium-rwo": "standard-csi"}})
	if pvc.Spec.VolumeName != "" {
		t.Fatal("volumeName must be cleared")
	}
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "standard-csi" {
		t.Fatalf("sc=%v", pvc.Spec.StorageClassName)
	}
	if _, ok := pvc.Annotations["volume.kubernetes.io/selected-node"]; ok {
		t.Fatal("selected-node remains")
	}
	if len(got) == 0 {
		t.Fatal("expected healer names")
	}
}

func TestPodSpecStripsZonePin(t *testing.T) {
	t.Parallel()
	spec := &corev1.PodSpec{
		NodeSelector: map[string]string{"topology.kubernetes.io/zone": "a"},
		Affinity:     &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}},
	}
	got := PodSpec(spec)
	if spec.NodeSelector != nil || spec.Affinity.NodeAffinity != nil {
		t.Fatal("pins remain")
	}
	if len(got) == 0 {
		t.Fatal("expected healers")
	}
}
