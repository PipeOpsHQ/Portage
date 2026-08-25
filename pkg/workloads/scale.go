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

package workloads

import (
	"context"
	"fmt"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"

	"github.com/PipeOpsHQ/portage/pkg/classify"
)

const originalReplicasAnn = "portage.io/original-replicas"

// Scale sets replicas, remembering the previous count for cutover rollback.
func Scale(ctx context.Context, kube kubernetes.Interface, w classify.Workload, replicas int32) error {
	switch w.Kind {
	case "StatefulSet":
		sts, err := kube.AppsV1().StatefulSets(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		cur := int32(1)
		if sts.Spec.Replicas != nil {
			cur = *sts.Spec.Replicas
		}
		if sts.Annotations == nil {
			sts.Annotations = map[string]string{}
		}
		if _, ok := sts.Annotations[originalReplicasAnn]; !ok {
			sts.Annotations[originalReplicasAnn] = strconv.Itoa(int(cur))
		}
		sts.Spec.Replicas = ptr.To(replicas)
		_, err = kube.AppsV1().StatefulSets(w.Namespace).Update(ctx, sts, metav1.UpdateOptions{})
		return err
	case "Deployment":
		dep, err := kube.AppsV1().Deployments(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		cur := int32(1)
		if dep.Spec.Replicas != nil {
			cur = *dep.Spec.Replicas
		}
		if dep.Annotations == nil {
			dep.Annotations = map[string]string{}
		}
		if _, ok := dep.Annotations[originalReplicasAnn]; !ok {
			dep.Annotations[originalReplicasAnn] = strconv.Itoa(int(cur))
		}
		dep.Spec.Replicas = ptr.To(replicas)
		_, err = kube.AppsV1().Deployments(w.Namespace).Update(ctx, dep, metav1.UpdateOptions{})
		return err
	default:
		return fmt.Errorf("scale: unsupported kind %s", w.Kind)
	}
}

// ScaledToZero is true when spec.replicas is 0 (source freeze).
func ScaledToZero(ctx context.Context, kube kubernetes.Interface, w classify.Workload) bool {
	switch w.Kind {
	case "StatefulSet":
		sts, err := kube.AppsV1().StatefulSets(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err != nil || sts.Spec.Replicas == nil {
			return false
		}
		return *sts.Spec.Replicas == 0
	case "Deployment":
		dep, err := kube.AppsV1().Deployments(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err != nil || dep.Spec.Replicas == nil {
			return false
		}
		return *dep.Spec.Replicas == 0
	default:
		return false
	}
}

// OriginalReplicas reads the freeze annotation (default 1).
func OriginalReplicas(ann map[string]string) int32 {
	if ann == nil {
		return 1
	}
	n, err := strconv.Atoi(ann[originalReplicasAnn])
	if err != nil || n < 0 {
		return 1
	}
	return int32(n)
}
