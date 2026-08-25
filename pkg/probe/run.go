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

package probe

import (
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes"

	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/kubeexec"
	"github.com/PipeOpsHQ/portage/pkg/movers"
	"github.com/PipeOpsHQ/portage/pkg/workloads"
)

// Run executes the class probe in a Ready pod. Kubernetes Ready is checked
// by the caller; this is the data probe (pg_isready, PING, …).
func Run(ctx context.Context, kube kubernetes.Interface, execer kubeexec.Interface, w classify.Workload) movers.ProbeResult {
	spec := Default(w)
	if len(spec.Command) == 0 {
		return movers.ProbeResult{OK: true, Message: spec.Message}
	}
	if execer == nil {
		return movers.ProbeResult{OK: false, Message: "execer not configured"}
	}
	pod, container, err := workloads.FirstReadyPod(ctx, kube, w)
	if err != nil {
		return movers.ProbeResult{OK: false, Message: err.Error()}
	}
	res, err := execer.Exec(ctx, w.Namespace, pod.Name, container, spec.Command)
	if err != nil {
		msg := spec.Message + " failed: " + err.Error()
		if res.Stderr != "" {
			msg += ": " + res.Stderr
		}
		return movers.ProbeResult{OK: false, Message: msg}
	}
	return movers.ProbeResult{OK: true, Message: spec.Message}
}

// LiveBytes execs SizeCommand and parses the integer. 0 means unknown/empty.
func LiveBytes(ctx context.Context, kube kubernetes.Interface, execer kubeexec.Interface, w classify.Workload) (int64, string, error) {
	cmd := SizeCommand(w)
	if len(cmd) == 0 {
		return 0, "no live size command", nil
	}
	pod, container, err := workloads.FirstReadyPod(ctx, kube, w)
	if err != nil {
		return 0, "", err
	}
	res, err := execer.Exec(ctx, w.Namespace, pod.Name, container, cmd)
	if err != nil {
		return 0, res.Stderr, fmt.Errorf("live size %s: %w", w.Key(), err)
	}
	return ParseBytes(res.Stdout), "live size", nil
}
