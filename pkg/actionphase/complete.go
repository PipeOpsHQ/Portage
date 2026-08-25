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

// Package actionphase is the Action completion gate: Kubernetes Ready is not
// enough; every stateful workload must also pass its class probe.
package actionphase

import (
	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
)

// CanSucceed reports whether an Action may legally enter phase Succeeded.
func CanSucceed(workloads []portagev1alpha1.WorkloadActionStatus) (bool, string) {
	if len(workloads) == 0 {
		return false, "no workloads recorded"
	}
	for _, w := range workloads {
		if !w.Ready {
			return false, w.Name + " is not Ready"
		}
		if w.Class == portagev1alpha1.ClassStateless {
			continue
		}
		if !w.ProbeOK {
			return false, w.Name + " class probe has not passed"
		}
	}
	return true, ""
}
