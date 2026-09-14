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

package clusters

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/rest"
)

// tokenSource mints a bearer token for the Kubernetes API.
type tokenSource interface {
	Token(ctx context.Context) (string, error)
}

func restConfig(host string, ca []byte, ts tokenSource) (*rest.Config, error) {
	host = strings.TrimSpace(host)
	if host == "" {
		return nil, fmt.Errorf("cluster API host is empty")
	}
	if !strings.Contains(host, "://") {
		host = "https://" + host
	}
	if len(ca) == 0 {
		return nil, fmt.Errorf("cluster CA is empty; refusing InsecureSkipTLSVerify")
	}
	if ts == nil {
		return nil, fmt.Errorf("token source is required")
	}
	cfg := &rest.Config{
		Host:            host,
		TLSClientConfig: rest.TLSClientConfig{CAData: ca},
		Timeout:         30 * time.Second,
	}
	cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		if rt == nil {
			rt = http.DefaultTransport
		}
		return &bearerTransport{base: rt, src: ts}
	})
	return cfg, nil
}

type bearerTransport struct {
	base http.RoundTripper
	src  tokenSource
}

func (t *bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	tok, err := t.src.Token(req.Context())
	if err != nil {
		return nil, err
	}
	r := req.Clone(req.Context())
	r.Header.Set("Authorization", "Bearer "+tok)
	return t.base.RoundTrip(r)
}

// cachedToken refreshes a bit before expiry so movers do not see 401s.
type cachedToken struct {
	mu    sync.Mutex
	tok   string
	exp   time.Time
	skew  time.Duration
	fetch func(context.Context) (string, time.Time, error)
}

func (c *cachedToken) Token(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	skew := c.skew
	if skew == 0 {
		skew = time.Minute
	}
	if c.tok != "" && time.Now().Add(skew).Before(c.exp) {
		return c.tok, nil
	}
	tok, exp, err := c.fetch(ctx)
	if err != nil {
		return "", err
	}
	c.tok, c.exp = tok, exp
	return tok, nil
}
