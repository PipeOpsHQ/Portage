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
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/PipeOpsHQ/portage/pkg/heal"
)

const hostnameLabel = "kubernetes.io/hostname"

// stripDestScheduling removes source-cluster node pins from dest workloads
// and deletes dest VolSync mover Jobs that AffinityFromVolume already stamped
// with a hostname that does not exist on dest. Direct copyMethod copies
// dest-pod nodeSelector onto the mover; dest GitOps often still carries the
// source kubernetes.io/hostname.
func (m Mover) stripDestScheduling(ctx context.Context, ns string) {
	dk := m.destKube()
	if dk == nil || !m.remoteDest() || ns == "" {
		return
	}
	nodes := destHostnames(ctx, dk)
	stripDestWorkloads(ctx, dk, ns)
	deletePinnedMoverJobs(ctx, dk, ns, nodes)
}

func destHostnames(ctx context.Context, kube kubernetes.Interface) map[string]struct{} {
	out := map[string]struct{}{}
	if kube == nil {
		return out
	}
	list, err := kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return out
	}
	for i := range list.Items {
		n := &list.Items[i]
		out[n.Name] = struct{}{}
		if h := n.Labels[hostnameLabel]; h != "" {
			out[h] = struct{}{}
		}
	}
	return out
}

func stripDestWorkloads(ctx context.Context, kube kubernetes.Interface, ns string) {
	if sts, err := kube.AppsV1().StatefulSets(ns).List(ctx, metav1.ListOptions{}); err == nil {
		for i := range sts.Items {
			o := &sts.Items[i]
			if len(heal.PodSpec(&o.Spec.Template.Spec)) == 0 {
				continue
			}
			_, _ = kube.AppsV1().StatefulSets(ns).Update(ctx, o, metav1.UpdateOptions{})
		}
	}
	if deps, err := kube.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{}); err == nil {
		for i := range deps.Items {
			o := &deps.Items[i]
			if len(heal.PodSpec(&o.Spec.Template.Spec)) == 0 {
				continue
			}
			_, _ = kube.AppsV1().Deployments(ns).Update(ctx, o, metav1.UpdateOptions{})
		}
	}
	if dss, err := kube.AppsV1().DaemonSets(ns).List(ctx, metav1.ListOptions{}); err == nil {
		for i := range dss.Items {
			o := &dss.Items[i]
			if len(heal.PodSpec(&o.Spec.Template.Spec)) == 0 {
				continue
			}
			_, _ = kube.AppsV1().DaemonSets(ns).Update(ctx, o, metav1.UpdateOptions{})
		}
	}
}

func deletePinnedMoverJobs(ctx context.Context, kube kubernetes.Interface, ns string, destNodes map[string]struct{}) {
	jobs, err := kube.BatchV1().Jobs(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}
	for i := range jobs.Items {
		j := &jobs.Items[i]
		if !isVolSyncMoverJob(j.Name) {
			continue
		}
		host := j.Spec.Template.Spec.NodeSelector[hostnameLabel]
		if host == "" {
			continue
		}
		if _, ok := destNodes[host]; ok {
			continue
		}
		_ = kube.BatchV1().Jobs(ns).Delete(ctx, j.Name, metav1.DeleteOptions{})
	}
}

func isVolSyncMoverJob(name string) bool {
	return strings.HasPrefix(name, "volsync-src-") || strings.HasPrefix(name, "volsync-dst-")
}

func (m Mover) destMoverPinMessage(ctx context.Context, ns string) string {
	if !m.remoteDest() {
		return ""
	}
	dk := m.destKube()
	if dk == nil || ns == "" {
		return ""
	}
	nodes := destHostnames(ctx, dk)
	jobs, err := dk.BatchV1().Jobs(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return ""
	}
	for i := range jobs.Items {
		j := &jobs.Items[i]
		if !isVolSyncMoverJob(j.Name) {
			continue
		}
		host := j.Spec.Template.Spec.NodeSelector[hostnameLabel]
		if host == "" {
			continue
		}
		if _, ok := nodes[host]; ok {
			continue
		}
		return fmt.Sprintf("dest mover job %s pinned to source-only node %s", j.Name, host)
	}
	return ""
}
