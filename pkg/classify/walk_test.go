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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
)

func TestWalkClassifiesSTSDeployAndOrphanPVC(t *testing.T) {
	t.Parallel()
	ns := "tenant-a"
	client := fake.NewSimpleClientset(
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "pg", Namespace: ns},
			Spec: appsv1.StatefulSetSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "pg", Image: "postgres:16"}},
						Volumes: []corev1.Volume{{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-pg"},
							},
						}},
					},
				},
			},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: ns},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "nginx", Image: "nginx:1.27"}},
					},
				},
			},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "cms", Namespace: ns},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "wp", Image: "wordpress:6"}},
						Volumes: []corev1.Volume{{
							Name: "uploads",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "uploads"},
							},
						}},
					},
				},
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "data-pg", Namespace: ns},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "uploads", Namespace: ns},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "orphan-disk", Namespace: ns},
		},
	)

	inv, err := Walk(context.Background(), client, []string{ns})
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]Workload{}
	for _, w := range inv.Workloads {
		byName[w.Kind+"/"+w.Name] = w
	}

	pg := byName["StatefulSet/pg"]
	if pg.Class != portagev1alpha1.ClassSQLLogical || pg.Engine != "postgres" {
		t.Fatalf("pg: %+v", pg)
	}
	web := byName["Deployment/web"]
	if web.Class != portagev1alpha1.ClassStateless {
		t.Fatalf("web: %+v", web)
	}
	cms := byName["Deployment/cms"]
	if cms.Class != portagev1alpha1.ClassGenericPVC {
		t.Fatalf("cms: %+v", cms)
	}
	orphan := byName["PersistentVolumeClaim/orphan-disk"]
	if orphan.Class != portagev1alpha1.ClassUnknownStateful || !orphan.Unclassified {
		t.Fatalf("orphan: %+v", orphan)
	}
	if _, ok := byName["PersistentVolumeClaim/data-pg"]; ok {
		t.Fatal("claimed PVC should not appear as its own workload")
	}
}
