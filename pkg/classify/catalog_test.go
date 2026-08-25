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

package classify

import (
	"testing"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
)

func TestMatchImage(t *testing.T) {
	t.Parallel()
	cases := []struct {
		image string
		want  string
		class portagev1alpha1.WorkloadClass
		ok    bool
	}{
		{"postgres:16", "postgres", portagev1alpha1.ClassSQLLogical, true},
		{"docker.io/bitnami/postgresql:15", "postgres", portagev1alpha1.ClassSQLLogical, true},
		{"timescale/timescaledb:latest-pg16", "timescale", portagev1alpha1.ClassSQLLogical, true},
		{"mariadb:11", "mariadb", portagev1alpha1.ClassSQLLogical, true},
		{"mysql:8.0", "mysql", portagev1alpha1.ClassSQLLogical, true},
		{"redis:7", "redis", portagev1alpha1.ClassKVLogical, true},
		{"minio/minio:RELEASE.2024-01-01", "minio", portagev1alpha1.ClassObjectStore, true},
		{"nginx:1.27", "", "", false},
		{"ghcr.io/myorg/custom-app:1", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.image, func(t *testing.T) {
			t.Parallel()
			got, ok := MatchImage(tc.image)
			if ok != tc.ok {
				t.Fatalf("ok=%v want %v (engine=%s)", ok, tc.ok, got.Name)
			}
			if !ok {
				return
			}
			if got.Name != tc.want {
				t.Fatalf("engine=%s want %s", got.Name, tc.want)
			}
			if got.Class != tc.class {
				t.Fatalf("class=%s want %s", got.Class, tc.class)
			}
		})
	}
}

func TestMatchImagePrefersMariaOverMySQL(t *testing.T) {
	t.Parallel()
	got, ok := MatchImage("bitnami/mariadb:11")
	if !ok || got.Name != "mariadb" {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

func TestMatchCRD(t *testing.T) {
	t.Parallel()
	got, ok := MatchCRD("postgresql.cnpg.io/Cluster")
	if !ok || got.Name != "postgres" {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}
