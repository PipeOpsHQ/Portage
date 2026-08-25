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

package actionphase

import (
	"testing"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
)

func TestCanSucceed(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   []portagev1alpha1.WorkloadActionStatus
		ok   bool
	}{
		{name: "empty", ok: false},
		{
			name: "velero-style ready without probe",
			in: []portagev1alpha1.WorkloadActionStatus{{
				Name: "pg", Class: portagev1alpha1.ClassSQLLogical, Ready: true, ProbeOK: false,
			}},
			ok: false,
		},
		{
			name: "ready and probed",
			in: []portagev1alpha1.WorkloadActionStatus{
				{Name: "pg", Class: portagev1alpha1.ClassSQLLogical, Ready: true, ProbeOK: true},
				{Name: "web", Class: portagev1alpha1.ClassStateless, Ready: true},
			},
			ok: true,
		},
		{
			name: "stateless not ready",
			in: []portagev1alpha1.WorkloadActionStatus{{
				Name: "web", Class: portagev1alpha1.ClassStateless, Ready: false,
			}},
			ok: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ok, _ := CanSucceed(tc.in)
			if ok != tc.ok {
				t.Fatalf("ok=%v want %v", ok, tc.ok)
			}
		})
	}
}
