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

// Package classify walks a namespace and assigns every workload a WorkloadClass.
// Unknown + PVC is UnknownStateful (still in the graph), never skipped.
package classify

import (
	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
)

// Workload is one classified unit of compute + optional durable state.
type Workload struct {
	Namespace    string
	Name         string
	Kind         string // StatefulSet, Deployment, DaemonSet, Job, CustomResource
	APIVersion   string
	Class        portagev1alpha1.WorkloadClass
	Engine       string
	Images       []string
	PVCNames     []string
	Unclassified bool
}

// Key is the stable inventory identity: namespace/kind/name.
func (w Workload) Key() string {
	return w.Namespace + "/" + w.Kind + "/" + w.Name
}

// Inventory is the classified graph of a namespace (or several).
type Inventory struct {
	Namespaces []string
	Workloads  []Workload
}

// Counts returns a breakdown by class.
func (inv Inventory) Counts() map[portagev1alpha1.WorkloadClass]int {
	out := map[portagev1alpha1.WorkloadClass]int{}
	for _, w := range inv.Workloads {
		out[w.Class]++
	}
	return out
}

// Stateful returns workloads that hold durable state.
func (inv Inventory) Stateful() []Workload {
	var out []Workload
	for _, w := range inv.Workloads {
		if w.Class != portagev1alpha1.ClassStateless {
			out = append(out, w)
		}
	}
	return out
}
