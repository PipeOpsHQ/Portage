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
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

type staticToken string

func (s staticToken) Token(context.Context) (string, error) { return string(s), nil }

func TestBearerTransportCancelsOnClientTimeout(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			http.Error(w, "auth", http.StatusUnauthorized)
			return
		}
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	client := &http.Client{
		Timeout: 200 * time.Millisecond,
		Transport: &bearerTransport{
			base: http.DefaultTransport,
			src:  staticToken("tok"),
		},
	}
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		_, _ = client.Do(req)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed-out request did not return; reconcile would stay wedged")
	}
}

type cancelRecorder struct {
	base     http.RoundTripper
	canceled *http.Request
}

func (c *cancelRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	if c.base != nil {
		return c.base.RoundTrip(req)
	}
	return nil, context.Canceled
}

func (c *cancelRecorder) CancelRequest(req *http.Request) { c.canceled = req }

func TestBearerTransportForwardsCancelRequest(t *testing.T) {
	t.Parallel()
	base := &cancelRecorder{}
	bt := &bearerTransport{base: base, src: staticToken("tok")}
	req, err := http.NewRequest(http.MethodGet, "https://example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	bt.CancelRequest(req)
	if base.canceled != req {
		t.Fatal("CancelRequest did not reach the wrapped transport")
	}
	if bt.WrappedRoundTripper() != base {
		t.Fatal("WrappedRoundTripper did not unwrap")
	}
}

func TestCachedTokenFetchDoesNotHoldLock(t *testing.T) {
	t.Parallel()
	var n atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	c := &cachedToken{
		skew: time.Minute,
		fetch: func(context.Context) (string, time.Time, error) {
			if n.Add(1) == 1 {
				close(entered)
				<-release
				return "slow", time.Now().Add(time.Hour), nil
			}
			return "fast", time.Now().Add(time.Hour), nil
		},
	}
	go func() { _, _ = c.Token(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first fetch did not start")
	}
	done := make(chan string, 1)
	go func() {
		tok, err := c.Token(context.Background())
		if err != nil {
			done <- err.Error()
			return
		}
		done <- tok
	}()
	select {
	case tok := <-done:
		if tok != "fast" {
			t.Fatalf("second caller got %q", tok)
		}
	case <-time.After(time.Second):
		t.Fatal("second Token call blocked on an in-flight refresh")
	}
	close(release)
}
