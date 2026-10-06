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
	"regexp"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/movers"
	"github.com/PipeOpsHQ/portage/pkg/usefulness"
)

var resticProcessed = regexp.MustCompile(`(?i)processed\s+\d+\s+files,\s+([0-9.]+)\s+([KMGT]?i?B)`)

func (m Mover) artifactFromSource(ctx context.Context, w classify.Workload, name string) (movers.Artifact, error) {
	art := movers.Artifact{Mover: m.Name()}
	obj, err := m.Dynamic.Resource(srcGVR).Namespace(w.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		art.Message = "volsync restic waiting for ReplicationSource"
		return art, nil
	}
	lastSync, _, _ := unstructured.NestedString(obj.Object, "status", "lastSyncTime")
	logs, _, _ := unstructured.NestedString(obj.Object, "status", "latestMoverStatus", "logs")
	size := parseResticBytes(logs)
	if lastSync == "" {
		art.SizeBytes = size
		art.Message = "volsync restic waiting for lastSyncTime"
		return art, nil
	}
	ev := usefulness.ForWorkload(w, usefulness.Input{
		SizeBytes:     size,
		HasSnapshot:   true,
		SnapshotReady: true,
	})
	ev.Mover = m.Name()
	ev.ID = lastSync
	if size > 0 {
		ev.SizeBytes = size
	}
	if ev.Message == "" || strings.HasPrefix(ev.Message, "CSI snapshot") {
		ev.Message = "volsync restic " + lastSync
	}
	return ev, nil
}

func parseResticBytes(logs string) int64 {
	m := resticProcessed.FindStringSubmatch(logs)
	if len(m) != 3 {
		return 0
	}
	n, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0
	}
	mult := int64(1)
	switch strings.ToLower(m[2]) {
	case "kib", "kb":
		mult = 1024
	case "mib", "mb":
		mult = 1024 * 1024
	case "gib", "gb":
		mult = 1024 * 1024 * 1024
	case "tib", "tb":
		mult = 1024 * 1024 * 1024 * 1024
	}
	return int64(n * float64(mult))
}
