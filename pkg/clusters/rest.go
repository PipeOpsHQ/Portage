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

	utilnet "k8s.io/apimachinery/pkg/util/net"
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

// client-go's User-Agent wrapper implements CancelRequest, so net/http treats
// the stack as a legacy RoundTripper: on Client.Timeout it closes Request.Cancel
// and calls CancelRequest instead of relying on context alone. http.Request.Clone
// drops that channel, and a wrapper that does not unwrap leaves the warning
// "unable to cancel request" with the in-flight call still running. The
// reconcile worker then never returns, so the Action is not retried until the
// process restarts.
var _ utilnet.RoundTripperWrapper = &bearerTransport{}

func (t *bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := req.Context().Err(); err != nil {
		return nil, err
	}
	tok, err := t.src.Token(req.Context())
	if err != nil {
		return nil, err
	}
	// CloneRequest keeps Request.Cancel. http.Request.Clone does not.
	r := utilnet.CloneRequest(req)
	r.Header.Set("Authorization", "Bearer "+tok)
	return t.base.RoundTrip(r)
}

func (t *bearerTransport) CancelRequest(req *http.Request) {
	cancelRequest(t.base, req)
}

func (t *bearerTransport) WrappedRoundTripper() http.RoundTripper { return t.base }

func cancelRequest(rt http.RoundTripper, req *http.Request) {
	type canceler interface {
		CancelRequest(*http.Request)
	}
	switch rt := rt.(type) {
	case canceler:
		rt.CancelRequest(req)
	case utilnet.RoundTripperWrapper:
		cancelRequest(rt.WrappedRoundTripper(), req)
	default:
		// Nothing left that understands CancelRequest. Request.Cancel on the
		// cloned request is what still unblocks *http.Transport.
	}
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
	skew := c.skew
	if skew == 0 {
		skew = time.Minute
	}
	if c.tok != "" && time.Now().Add(skew).Before(c.exp) {
		tok := c.tok
		c.mu.Unlock()
		return tok, nil
	}
	fetch := c.fetch
	c.mu.Unlock()
	// fetch runs outside the lock. A refresh that ignores cancellation used
	// to pin every later caller, including the reconcile, until process restart.
	if err := ctx.Err(); err != nil {
		return "", err
	}
	tok, exp, err := fetch(ctx)
	if err != nil {
		return "", err
	}
	c.mu.Lock()
	c.tok, c.exp = tok, exp
	c.mu.Unlock()
	return tok, nil
}
