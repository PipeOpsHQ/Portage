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

package render

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/transform"
)

func TestForSelectsKind(t *testing.T) {
	t.Parallel()
	if For(nil, transform.Options{}).Kind() != portagev1alpha1.RendererSanitize {
		t.Fatal("default sanitize")
	}
	git := For(&portagev1alpha1.Policy{Spec: portagev1alpha1.PolicySpec{
		Renderer: portagev1alpha1.RendererSpec{Kind: portagev1alpha1.RendererGit, Git: &portagev1alpha1.GitSource{URL: "/tmp"}},
	}}, transform.Options{})
	if git.Kind() != portagev1alpha1.RendererGit {
		t.Fatalf("got %s", git.Kind())
	}
	wh := For(&portagev1alpha1.Policy{Spec: portagev1alpha1.PolicySpec{
		Renderer: portagev1alpha1.RendererSpec{Kind: portagev1alpha1.RendererWebhook, WebhookURL: "http://x"},
	}}, transform.Options{})
	if wh.Kind() != portagev1alpha1.RendererWebhook {
		t.Fatalf("got %s", wh.Kind())
	}
}

func TestGitLoadsLocalTreeAndStripsZone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "sts.yaml")
	if err := os.WriteFile(path, []byte(`
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: pg
  namespace: ns
  annotations:
    volume.kubernetes.io/selected-node: ip-1
spec: {}
`), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := Git{Source: &portagev1alpha1.GitSource{URL: dir}}.Render(context.Background(), Request{})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].GetName() != "pg" {
		t.Fatalf("%+v", out)
	}
	ann := out[0].GetAnnotations()
	if _, found := ann["volume.kubernetes.io/selected-node"]; found {
		t.Fatal("zone/node pin must be stripped")
	}
}

func TestWebhookPostsAndSanitizes(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req webhookRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		_ = json.NewEncoder(w).Encode(webhookResponse{Objects: []map[string]any{{
			"apiVersion": "v1",
			"kind":       "PersistentVolumeClaim",
			"metadata": map[string]any{
				"name": "data-pg",
				"annotations": map[string]any{
					"volume.kubernetes.io/selected-node": "ip-9",
				},
			},
		}}})
	}))
	defer srv.Close()
	out, err := Webhook{URL: srv.URL}.Render(context.Background(), Request{
		Policy: &portagev1alpha1.Policy{ObjectMeta: metav1.ObjectMeta{Name: "p"}},
		SourceObjects: []*unstructured.Unstructured{{Object: map[string]any{
			"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "x"},
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].GetKind() != "PersistentVolumeClaim" {
		t.Fatalf("%+v", out)
	}
	ann := out[0].GetAnnotations()
	if _, found := ann["volume.kubernetes.io/selected-node"]; found {
		t.Fatal("webhook output must still be sanitized")
	}
}
