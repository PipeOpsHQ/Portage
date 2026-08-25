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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/transform"
)

// Webhook POSTs source objects to an external renderer and sanitizes the result.
type Webhook struct {
	URL     string
	Client  *http.Client
	Options transform.Options
}

func (Webhook) Kind() portagev1alpha1.RendererKind { return portagev1alpha1.RendererWebhook }

type webhookRequest struct {
	Policy  string           `json:"policy"`
	Objects []map[string]any `json:"objects"`
}

type webhookResponse struct {
	Objects []map[string]any `json:"objects"`
}

func (w Webhook) Render(ctx context.Context, req Request) ([]*unstructured.Unstructured, error) {
	url := w.URL
	if url == "" && req.Policy != nil {
		url = req.Policy.Spec.Renderer.WebhookURL
	}
	if url == "" {
		return nil, fmt.Errorf("webhook renderer: spec.renderer.webhookURL is required")
	}
	payload := webhookRequest{Objects: make([]map[string]any, 0, len(req.SourceObjects))}
	if req.Policy != nil {
		payload.Policy = req.Policy.Name
	}
	for _, o := range req.SourceObjects {
		payload.Objects = append(payload.Objects, o.UnstructuredContent())
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	c := w.Client
	if c == nil {
		c = &http.Client{Timeout: 30 * time.Second}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	res, err := c.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("webhook renderer: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return nil, fmt.Errorf("webhook renderer: unexpected status %s", res.Status)
	}
	var decoded webhookResponse
	if err := json.NewDecoder(res.Body).Decode(&decoded); err != nil {
		return nil, fmt.Errorf("webhook renderer: decode: %w", err)
	}
	opt := w.Options
	if req.Pair != nil && opt.StorageClassMap == nil {
		opt.StorageClassMap = req.Pair.Spec.StorageClassMap
	}
	out := make([]*unstructured.Unstructured, 0, len(decoded.Objects))
	for _, raw := range decoded.Objects {
		obj := &unstructured.Unstructured{Object: raw}
		transform.Object(obj, opt)
		out = append(out, obj)
	}
	return out, nil
}
