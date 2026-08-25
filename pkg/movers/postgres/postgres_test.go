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

package postgres

import (
	"context"
	"testing"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/movers"
)

func TestDiscoverOnlyPostgres(t *testing.T) {
	t.Parallel()
	m := Mover{}
	cap, err := m.Discover(context.Background(), classify.Workload{Engine: "mysql", Class: portagev1alpha1.ClassSQLLogical})
	if err != nil || cap.Replicate {
		t.Fatalf("mysql must not use postgres-streaming: %+v", cap)
	}
	cap, err = m.Discover(context.Background(), classify.Workload{Engine: "postgres", Class: portagev1alpha1.ClassSQLLogical})
	if err != nil || !cap.Replicate {
		t.Fatalf("postgres: %+v err=%v", cap, err)
	}
}

func TestName(t *testing.T) {
	t.Parallel()
	if (Mover{}).Name() != "postgres-streaming" {
		t.Fatal("name")
	}
	_ = movers.Mover(Mover{})
}
