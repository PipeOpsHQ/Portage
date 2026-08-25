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

package backup

import (
	"testing"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
)

func TestHealthyRequiresUsefulStateful(t *testing.T) {
	t.Parallel()
	inv := classify.Inventory{Workloads: []classify.Workload{
		{Namespace: "ns", Kind: "Deployment", Name: "web", Class: portagev1alpha1.ClassStateless},
		{Namespace: "ns", Kind: "StatefulSet", Name: "pg", Class: portagev1alpha1.ClassSQLLogical, Engine: "postgres"},
	}}
	ok, missing := Healthy(inv, nil)
	if ok || len(missing) != 1 {
		t.Fatalf("ok=%v missing=%v", ok, missing)
	}
	ok, _ = Healthy(inv, []portagev1alpha1.ArtifactHealth{{
		Workload:  "ns/StatefulSet/pg",
		Useful:    true,
		SizeBytes: 2 << 20,
	}})
	if !ok {
		t.Fatal("expected healthy")
	}
	ok, _ = Healthy(inv, []portagev1alpha1.ArtifactHealth{{
		Workload:  "ns/StatefulSet/pg",
		Useful:    false,
		SizeBytes: 11800,
		Message:   "certs-only",
	}})
	if ok {
		t.Fatal("certs-only must not be healthy")
	}
}
