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

// Package traffic is the cutover traffic-switch plugin.
// Core ships Noop and a generic HTTP webhook. DNS, service-mesh, and
// platform routers are out-of-tree Hooks.
package traffic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Event is sent to a Hook at promote time.
type Event struct {
	Action      string `json:"action"` // switch | rollback
	Policy      string `json:"policy"`
	Source      string `json:"source"`
	Destination string `json:"destination"`
}

// Hook switches or rolls back user traffic.
type Hook interface {
	Name() string
	Switch(ctx context.Context, ev Event) error
	Rollback(ctx context.Context, ev Event) error
}

// Noop is the default: data promote without moving DNS/ingress.
type Noop struct{}

func (Noop) Name() string                          { return "noop" }
func (Noop) Switch(context.Context, Event) error   { return nil }
func (Noop) Rollback(context.Context, Event) error { return nil }

// Webhook POSTs the Event JSON to URL.
type Webhook struct {
	URL    string
	Client *http.Client
}

func (w Webhook) Name() string { return "webhook" }

func (w Webhook) Switch(ctx context.Context, ev Event) error {
	return w.post(ctx, ev)
}

func (w Webhook) Rollback(ctx context.Context, ev Event) error {
	ev.Action = "rollback"
	return w.post(ctx, ev)
}

func (w Webhook) post(ctx context.Context, ev Event) error {
	if w.URL == "" {
		return fmt.Errorf("traffic webhook: empty URL")
	}
	body, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	c := w.Client
	if c == nil {
		c = &http.Client{Timeout: 15 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.Do(req)
	if err != nil {
		return fmt.Errorf("traffic webhook: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return fmt.Errorf("traffic webhook: unexpected status %s", res.Status)
	}
	return nil
}
