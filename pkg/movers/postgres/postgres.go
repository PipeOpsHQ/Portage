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
	"k8s.io/client-go/kubernetes"
)

// PromoteCmd is executed on the destination primary-to-be.
var PromoteCmd = []string{"/bin/sh", "-c", `if command -v psql >/dev/null; then psql -U postgres -c "SELECT pg_promote();"; else pg_ctl promote; fi`}

// ReplicaLagCmd prints replay lag in seconds (0 = caught up).
var ReplicaLagCmd = []string{"/bin/sh", "-c", `psql -U postgres -tAc "SELECT COALESCE(EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::int, 0);"`}

// Mover uses in-pod psql/pg_ctl. VolSync is the fallback for generic PVCs.
type Mover struct {
	Kube kubernetes.Interface
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

func (m Mover) Replicate(context.Context, classify.Workload, movers.ClusterHandle, movers.ClusterHandle) error {
	// Dest standby is provisioned by the renderer; we only promote/quiesce here.
	return nil
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
	if m.Exec == nil || m.Kube == nil {
		return fmt.Errorf("postgres-streaming: kube/exec required to promote")
	}
	if err := workloads.Scale(ctx, m.Kube, w, 1); err != nil {
		return err
	}
	pod, container, err := workloads.FirstReadyPod(ctx, m.Kube, w)
	if err != nil {
		return fmt.Errorf("promote: %w", err)
	}
	_, err = m.Exec.Exec(ctx, w.Namespace, pod.Name, container, PromoteCmd)
	return err
}

func (m Mover) Probe(ctx context.Context, w classify.Workload, _ movers.ClusterHandle) (movers.ProbeResult, error) {
	if m.Exec == nil || m.Kube == nil {
		return movers.ProbeResult{OK: false, Message: "execer not configured"}, nil
	}
	pod, container, err := workloads.FirstReadyPod(ctx, m.Kube, w)
	if err != nil {
		return movers.ProbeResult{OK: false, Message: err.Error()}, nil
	}
	_, err = m.Exec.Exec(ctx, w.Namespace, pod.Name, container, []string{"pg_isready"})
	if err != nil {
		return movers.ProbeResult{OK: false, Message: "pg_isready: " + err.Error()}, nil
	}
	return movers.ProbeResult{OK: true, Message: "pg_isready"}, nil
}

// LagSeconds execs ReplicaLagCmd. Missing execer → unknown (not zero).
func (m Mover) LagSeconds(ctx context.Context, w classify.Workload) (int, bool, error) {
	if m.Exec == nil || m.Kube == nil {
		return 0, false, nil
	}
	pod, container, err := workloads.FirstReadyPod(ctx, m.Kube, w)
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
