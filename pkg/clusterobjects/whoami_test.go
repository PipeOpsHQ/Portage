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

package clusterobjects

import "testing"

func TestSubjectFromUsername(t *testing.T) {
	t.Parallel()
	cases := []struct {
		username string
		kind     string
		name     string
	}{
		{username: "", kind: "", name: ""},
		{
			username: "system:serviceaccount:portage-system:portage-controller",
			kind:     "ServiceAccount",
			name:     "system:serviceaccount:portage-system:portage-controller",
		},
		{
			username: "11111111-2222-3333-4444-555555555555",
			kind:     "User",
			name:     "11111111-2222-3333-4444-555555555555",
		},
		{username: "clusterUser", kind: "User", name: "clusterUser"},
	}
	for _, tc := range cases {
		got := subjectFromUsername(tc.username)
		if got.Kind != tc.kind || got.Name != tc.name {
			t.Fatalf("%q: got %#v", tc.username, got)
		}
	}
}
