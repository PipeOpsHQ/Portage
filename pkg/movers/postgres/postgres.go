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

// Package postgres is the streaming-replica mover for SQLLogical Postgres.
// Volume rsync of pgdata is not this path.
package postgres

import (
	"context"
	"fmt"
	"strings"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/kubeexec"
	"github.com/PipeOpsHQ/portage/pkg/movers"
	"github.com/PipeOpsHQ/portage/pkg/workloads"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// PromoteCmd is executed on the destination primary-to-be.
var PromoteCmd = []string{"/bin/sh", "-c", `if command -v psql >/dev/null; then psql -U postgres -c "SELECT pg_promote();"; else pg_ctl promote; fi`}

// ReplicaLagCmd prints replay lag in seconds (0 = caught up).
var ReplicaLagCmd = []string{"/bin/sh", "-c", `psql -U postgres -tAc "SELECT COALESCE(EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::int, 0);"`}

// Mover uses in-pod psql/pg_ctl. VolSync is the fallback for generic PVCs.
type Mover struct {
	Kube kubernetes.Interface // source
	Dest kubernetes.Interface // dest; nil ⇒ dest is Kube
	Exec kubeexec.Interface
}

func (m Mover) Name() string { return "postgres-streaming" }

func (m Mover) Classes() []portagev1alpha1.WorkloadClass {
	return []portagev1alpha1.WorkloadClass{portagev1alpha1.ClassSQLLogical}
}

func (m Mover) Discover(_ context.Context, w classify.Workload) (movers.Capability, error) {
	if w.Engine != "postgres" && w.Engine != "timescale" {
		return movers.Capability{}, nil
	}
	return movers.Capability{Replicate: true, Restore: true, Backup: false}, nil
}

func (m Mover) Backup(context.Context, classify.Workload, movers.ClusterHandle) (movers.Artifact, error) {
	return movers.Artifact{Mover: m.Name(), Message: "use logical dump + CSI for backup"}, nil
}

func (m Mover) Replicate(ctx context.Context, w classify.Workload, src, dst movers.ClusterHandle) error {
	dest := m.Dest
	if dest == nil {
		dest = m.Kube
	}
	if dest == nil {
		return fmt.Errorf("postgres-streaming: dest kube required")
	}
	host := src.Name
	if host == "" {
		host = w.Name
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "portage-standby-" + w.Name,
			Namespace: w.Namespace,
			Labels:    map[string]string{"portage.io/name": w.Name, "portage.io/role": "standby"},
		},
		Data: map[string]string{
			"primary_conninfo": fmt.Sprintf("host=%s user=postgres application_name=portage", host),
			"primary_cluster":  dst.Name,
			"hot_standby":      "on",
		},
	}
	_, err := dest.CoreV1().ConfigMaps(w.Namespace).Create(ctx, cm, metav1.CreateOptions{})
	if err != nil {
		_, err = dest.CoreV1().ConfigMaps(w.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
	}
	if err != nil {
		return err
	}
	if err := m.ensurePrimaryService(ctx, w); err != nil {
		return err
	}
	_ = m.ensureReplicatorRole(ctx, w)
	primary := svcName + "." + w.Namespace + ".svc"
	if src.Name != "" && src.Name != "local" && src.Name != "source" {
		primary = src.Name
	}
	if err := m.ensureBasebackupJob(ctx, w, primary); err != nil {
		return err
	}
	return m.patchDestStandby(ctx, w)
}

func (m Mover) Restore(context.Context, classify.Workload, movers.Artifact, movers.ClusterHandle) error {
	return nil
}

func (m Mover) Quiesce(ctx context.Context, w classify.Workload) error {
	if m.Kube == nil {
		return nil
	}
	return workloads.Scale(ctx, m.Kube, w, 0)
}

func (m Mover) Promote(ctx context.Context, w classify.Workload, _ movers.ClusterHandle) error {
	dest := m.destKube()
	if m.Exec == nil || dest == nil {
		return fmt.Errorf("postgres-streaming: dest kube/exec required to promote")
	}
	if err := workloads.Scale(ctx, dest, w, 1); err != nil {
		return err
	}
	pod, container, err := workloads.FirstReadyPod(ctx, dest, w)
	if err != nil {
		return fmt.Errorf("promote: %w", err)
	}
	_, err = m.Exec.Exec(ctx, w.Namespace, pod.Name, container, PromoteCmd)
	return err
}

func (m Mover) destKube() kubernetes.Interface {
	if m.Dest != nil {
		return m.Dest
	}
	return m.Kube
}

func (m Mover) Probe(ctx context.Context, w classify.Workload, _ movers.ClusterHandle) (movers.ProbeResult, error) {
	dest := m.destKube()
	if dest == nil {
		return movers.ProbeResult{OK: false, Message: "postgres-streaming: dest kube required"}, nil
	}
	if m.Exec != nil {
		if pod, container, err := workloads.FirstReadyPod(ctx, dest, w); err == nil {
			if _, err := m.Exec.Exec(ctx, w.Namespace, pod.Name, container, []string{"pg_isready"}); err == nil {
				return movers.ProbeResult{OK: true, Message: "pg_isready"}, nil
			}
		}
	}
	if _, err := dest.CoreV1().ConfigMaps(w.Namespace).Get(ctx, "portage-standby-"+w.Name, metav1.GetOptions{}); err != nil {
		return movers.ProbeResult{OK: false, Message: "standby config missing"}, nil
	}
	job, err := dest.BatchV1().Jobs(w.Namespace).Get(ctx, "portage-basebackup-"+w.Name, metav1.GetOptions{})
	if err != nil {
		return movers.ProbeResult{OK: false, Message: "basebackup job missing"}, nil
	}
	if job.Status.Succeeded < 1 {
		return movers.ProbeResult{OK: false, Message: "waiting for pg_basebackup"}, nil
	}
	return movers.ProbeResult{OK: true, Message: "basebackup complete; standby configured"}, nil
}

// LagSeconds execs ReplicaLagCmd on the dest standby. Missing execer → unknown (not zero).
func (m Mover) LagSeconds(ctx context.Context, w classify.Workload) (int, bool, error) {
	kube := m.destKube()
	if m.Exec == nil || kube == nil {
		return 0, false, nil
	}
	pod, container, err := workloads.FirstReadyPod(ctx, kube, w)
	if err != nil {
		return 0, false, err
	}
	res, err := m.Exec.Exec(ctx, w.Namespace, pod.Name, container, ReplicaLagCmd)
	if err != nil {
		return 0, false, err
	}
	n := 0
	fmt.Sscanf(strings.TrimSpace(res.Stdout), "%d", &n)
	return n, true, nil
}
