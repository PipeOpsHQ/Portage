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
	"testing"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
)

func TestParseBytes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want int64
	}{
		{"11800\n", 11800},
		{"  2097152 ", 2097152},
		{"", 0},
		{"not-a-number", 0},
	}
	for _, tc := range cases {
		if got := ParseBytes(tc.in); got != tc.want {
			t.Fatalf("ParseBytes(%q)=%d want %d", tc.in, got, tc.want)
		}
	}
}

func TestSizeCommandPostgres(t *testing.T) {
	t.Parallel()
	cmd := SizeCommand(classify.Workload{Engine: "postgres", Class: portagev1alpha1.ClassSQLLogical})
	if len(cmd) == 0 {
		t.Fatal("expected postgres size command")
	}
}

func TestDefaultPostgresIsPgIsReady(t *testing.T) {
	t.Parallel()
	spec := Default(classify.Workload{Engine: "postgres"})
	if spec.Message != "pg_isready" || spec.Command[0] != "pg_isready" {
		t.Fatalf("%+v", spec)
	}
}
