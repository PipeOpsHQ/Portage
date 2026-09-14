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

package webhook

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/movers"
)

func TestWebhookBackupRestore(t *testing.T) {
	t.Parallel()
	var last request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &last)
		switch last.Operation {
		case "backup":
			_ = json.NewEncoder(w).Encode(response{Artifact: &movers.Artifact{ID: "velero/b1", SizeBytes: 99, Useful: true}})
		case "restore":
			_ = json.NewEncoder(w).Encode(response{})
		default:
			http.Error(w, "nope", 400)
		}
	}))
	t.Cleanup(srv.Close)
	m := Mover{Plugin: portagev1alpha1.Plugin{
		ObjectMeta: metav1.ObjectMeta{Name: "velero"},
		Spec:       portagev1alpha1.PluginSpec{WebhookURL: srv.URL, Backup: true, Restore: true},
	}}
	w := classify.Workload{Namespace: "ns", Name: "data", Kind: "PersistentVolumeClaim", Class: portagev1alpha1.ClassGenericPVC, PVCNames: []string{"data"}}
	art, err := m.Backup(context.Background(), w, movers.ClusterHandle{Name: "dst"})
	if err != nil {
		t.Fatal(err)
	}
	if last.Operation != "backup" || last.Plugin != "velero" || last.Workload.Name != "data" {
		t.Fatalf("backup req %+v", last)
	}
	if art.ID != "velero/b1" || !art.Useful || art.Mover != "velero" {
		t.Fatalf("artifact %+v", art)
	}
	if err := m.Restore(context.Background(), w, art, movers.ClusterHandle{Name: "dst"}); err != nil {
		t.Fatal(err)
	}
	if last.Operation != "restore" || last.Artifact == nil || last.Artifact.ID != "velero/b1" {
		t.Fatalf("restore req %+v", last)
	}
}

func TestWebhookError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(response{Error: "restic repo locked"})
	}))
	t.Cleanup(srv.Close)
	m := Mover{Plugin: portagev1alpha1.Plugin{
		ObjectMeta: metav1.ObjectMeta{Name: "restic"},
		Spec:       portagev1alpha1.PluginSpec{WebhookURL: srv.URL},
	}}
	_, err := m.Backup(context.Background(), classify.Workload{Name: "x"}, movers.ClusterHandle{})
	if err == nil || err.Error() == "" {
		t.Fatal("expected plugin error")
	}
}

func TestDiscoverDefaultsBackupRestore(t *testing.T) {
	t.Parallel()
	m := Mover{Plugin: portagev1alpha1.Plugin{ObjectMeta: metav1.ObjectMeta{Name: "x"}}}
	cap, err := m.Discover(context.Background(), classify.Workload{})
	if err != nil {
		t.Fatal(err)
	}
	if !cap.Backup || !cap.Restore || cap.Replicate {
		t.Fatalf("%+v", cap)
	}
}
