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

package usefulness

import (
	"testing"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
)

func TestEvaluateCertsOnlyPostgresIsNotUseful(t *testing.T) {
	t.Parallel()
	got := Evaluate(Input{
		Class:     portagev1alpha1.ClassSQLLogical,
		Engine:    "postgres",
		SizeBytes: 11800,
		Files: []File{
			{Path: "certs/tls.crt", SizeBytes: 6000},
			{Path: "certs/tls.key", SizeBytes: 5800},
		},
	})
	if got.Useful {
		t.Fatalf("12KiB certs-only must not be useful: %+v", got)
	}
}

func TestEvaluateSQLDumpIsUseful(t *testing.T) {
	t.Parallel()
	got := Evaluate(Input{
		Class:  portagev1alpha1.ClassSQLLogical,
		Engine: "postgres",
		Files: []File{
			{Path: "prebackup/pg.sql", SizeBytes: 2 * 1024 * 1024},
		},
	})
	if !got.Useful {
		t.Fatalf("sql dump should be useful: %+v", got)
	}
}

func TestEvaluateLiveDatadirDoesNotSaveTinyDump(t *testing.T) {
	t.Parallel()
	got := Evaluate(Input{
		Class:     portagev1alpha1.ClassSQLLogical,
		Engine:    "postgres",
		SizeBytes: 40 << 20, // empty PGDATA is large
		Files:     []File{{Path: "dump.sql", SizeBytes: 3000}},
	})
	if got.Useful {
		t.Fatalf("tiny dump must not be rescued by live datadir size: %+v", got)
	}
}

func TestEvaluateSnapshotAloneDoesNotSaveLogicalEngine(t *testing.T) {
	t.Parallel()
	got := Evaluate(Input{
		Class:         portagev1alpha1.ClassSQLLogical,
		Engine:        "postgres",
		HasSnapshot:   true,
		SnapshotReady: true,
	})
	if got.Useful {
		t.Fatal("ReadyToUse snapshot without dump size must not pass logical engines")
	}
}

func TestEvaluateGenericPVCSnapshotIsUseful(t *testing.T) {
	t.Parallel()
	got := Evaluate(Input{
		Class:         portagev1alpha1.ClassGenericPVC,
		HasSnapshot:   true,
		SnapshotReady: true,
	})
	if !got.Useful {
		t.Fatalf("generic PVC snapshot: %+v", got)
	}
}

func TestEvaluateTinyVolumeNotUseful(t *testing.T) {
	t.Parallel()
	got := Evaluate(Input{
		Class:     portagev1alpha1.ClassSearchFS,
		Engine:    "elasticsearch",
		SizeBytes: 4096,
	})
	if got.Useful {
		t.Fatalf("tiny volume: %+v", got)
	}
}
