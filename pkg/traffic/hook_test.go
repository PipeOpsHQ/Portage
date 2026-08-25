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

package traffic

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWebhookSwitchAndRollback(t *testing.T) {
	t.Parallel()
	var got Event
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &got)
		w.WriteHeader(204)
	}))
	t.Cleanup(srv.Close)
	h := Webhook{URL: srv.URL, Client: srv.Client()}
	ev := Event{Action: "switch", Policy: "p", Source: "a", Destination: "b"}
	if err := h.Switch(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	if got.Action != "switch" || got.Destination != "b" {
		t.Fatalf("%+v", got)
	}
	if err := h.Rollback(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	if got.Action != "rollback" {
		t.Fatalf("rollback action=%s", got.Action)
	}
}

func TestWebhookRejectsNon2xx(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
	}))
	t.Cleanup(srv.Close)
	h := Webhook{URL: srv.URL, Client: srv.Client()}
	if err := h.Switch(context.Background(), Event{Action: "switch"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestNoop(t *testing.T) {
	t.Parallel()
	var n Noop
	if err := n.Switch(context.Background(), Event{}); err != nil {
		t.Fatal(err)
	}
}
