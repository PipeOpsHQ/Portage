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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/PipeOpsHQ/portage/pkg/classify"
)

// Ready reports whether the owning controller has a Ready replica.
func Ready(ctx context.Context, kube kubernetes.Interface, w classify.Workload) (bool, error) {
	switch w.Kind {
	case "StatefulSet":
		sts, err := kube.AppsV1().StatefulSets(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		want := int32(1)
		if sts.Spec.Replicas != nil {
			want = *sts.Spec.Replicas
		}
		if want == 0 {
			return false, nil
		}
		return sts.Status.ReadyReplicas >= 1, nil
	case "Deployment":
		dep, err := kube.AppsV1().Deployments(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		want := int32(1)
		if dep.Spec.Replicas != nil {
			want = *dep.Spec.Replicas
		}
		if want == 0 {
			return false, nil
		}
		return dep.Status.ReadyReplicas >= 1, nil
	case "DaemonSet":
		ds, err := kube.AppsV1().DaemonSets(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return ds.Status.NumberReady >= 1, nil
	default:
		pods, err := ListPods(ctx, kube, w)
		if err != nil {
			return false, err
		}
		for _, p := range pods {
			if isPodReady(&p) {
				return true, nil
			}
		}
		return false, nil
	}
}

// ListPods returns pods owned by the workload (or named like an STS ordinal).
func ListPods(ctx context.Context, kube kubernetes.Interface, w classify.Workload) ([]corev1.Pod, error) {
	list, err := kube.CoreV1().Pods(w.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list pods in %s: %w", w.Namespace, err)
	}
	owners := map[string]struct{}{w.Name: {}}
	if w.Kind == "Deployment" {
		rss, err := kube.AppsV1().ReplicaSets(w.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		for _, rs := range rss.Items {
			if ownedBy(&rs, "Deployment", w.Name) {
				owners[rs.Name] = struct{}{}
			}
		}
	}
	var out []corev1.Pod
	for i := range list.Items {
		p := list.Items[i]
		if podOwnedBy(p, owners) {
			out = append(out, p)
		}
	}
	return out, nil
}

// FirstReadyPod returns a Ready pod and a container name to exec into.
func FirstReadyPod(ctx context.Context, kube kubernetes.Interface, w classify.Workload) (*corev1.Pod, string, error) {
	pods, err := ListPods(ctx, kube, w)
	if err != nil {
		return nil, "", err
	}
	for i := range pods {
		p := &pods[i]
		if !isPodReady(p) {
			continue
		}
		name := containerName(p)
		return p, name, nil
	}
	if len(pods) == 0 {
		return nil, "", fmt.Errorf("no pods for %s", w.Key())
	}
	p := &pods[0]
	return p, containerName(p), fmt.Errorf("%s has no Ready pod", w.Key())
}

func isPodReady(p *corev1.Pod) bool {
	if p.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func containerName(p *corev1.Pod) string {
	if len(p.Spec.Containers) == 0 {
		return ""
	}
	return p.Spec.Containers[0].Name
}

func ownedBy(rs *appsv1.ReplicaSet, kind, name string) bool {
	for _, o := range rs.OwnerReferences {
		if o.Kind == kind && o.Name == name {
			return true
		}
	}
	return false
}

func podOwnedBy(p corev1.Pod, names map[string]struct{}) bool {
	for _, o := range p.OwnerReferences {
		if _, ok := names[o.Name]; ok {
			return true
		}
	}
	return false
}
