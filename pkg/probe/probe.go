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

// Package probe defines class-level "is the data real" checks.
// A restore/cutover Action must not reach Succeeded unless every stateful
// workload's probe passed. Kubernetes Ready is a necessary but not
// sufficient signal (pods can be Ready on an empty volume).
package probe

import (
	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/movers"
)

// Spec describes the default probe for a class/engine. Movers may override.
type Spec struct {
	Command []string
	Message string
}

// Default returns a suggested in-container command. Execution is the mover's job.
func Default(w classify.Workload) Spec {
	switch w.Engine {
	case "postgres", "timescale":
		return Spec{Command: []string{"pg_isready"}, Message: "pg_isready"}
	case "mysql", "mariadb":
		return Spec{Command: []string{"mysqladmin", "ping"}, Message: "mysqladmin ping"}
	case "redis", "valkey", "keydb", "dragonfly":
		return Spec{Command: []string{"redis-cli", "ping"}, Message: "PING"}
	case "mongo":
		return Spec{Command: []string{"mongosh", "--eval", "db.hello()"}, Message: "hello"}
	case "minio":
		return Spec{Command: []string{"mc", "ready", "local"}, Message: "mc ready"}
	}
	switch w.Class {
	case portagev1alpha1.ClassStateless:
		return Spec{Message: "k8s-ready"}
	case portagev1alpha1.ClassClusterObjects:
		return Spec{Message: "dest-get"}
	default:
		return Spec{Message: "volume-mounted"}
	}
}

// RequireOK is the Action completion gate.
func RequireOK(results []movers.ProbeResult) bool {
	if len(results) == 0 {
		return false
	}
	for _, r := range results {
		if !r.OK {
			return false
		}
	}
	return true
}
