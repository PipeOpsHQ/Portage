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

// Package backup rolls usefulness into Policy.status.backupHealthy.
package backup

import (
	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
)

// Healthy is false when any stateful workload lacks a useful artifact.
// Stateless workloads are ignored. No stateful workloads → healthy.
func Healthy(inv classify.Inventory, artifacts []portagev1alpha1.ArtifactHealth) (bool, []string) {
	byKey := map[string]portagev1alpha1.ArtifactHealth{}
	for _, a := range artifacts {
		byKey[a.Workload] = a
	}
	var missing []string
	for _, w := range inv.Stateful() {
		a, ok := byKey[w.Key()]
		if !ok || !a.Useful {
			missing = append(missing, w.Key())
		}
	}
	return len(missing) == 0, missing
}

// FromArtifact converts a mover artifact into Policy status.
func FromArtifact(w classify.Workload, useful bool, size int64, msg, id string) portagev1alpha1.ArtifactHealth {
	return portagev1alpha1.ArtifactHealth{
		Workload:   w.Key(),
		Useful:     useful,
		SizeBytes:  size,
		Message:    msg,
		ArtifactID: id,
	}
}
