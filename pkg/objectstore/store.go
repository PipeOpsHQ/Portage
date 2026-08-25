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

// Package objectstore is the portable artifact plane (dumps that survive a
// cloud boundary). CSI snapshots do not.
package objectstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Store is Put/Get of opaque backup blobs. Implementations: Memory (tests),
// Dir (hub PVC), S3 (production via FromEnv).
type Store interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
}

// Memory is an in-process store for tests and dual-cluster fakes.
type Memory struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (m *Memory) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data == nil {
		m.data = map[string][]byte{}
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	m.data[key] = cp
	return nil
}

func (m *Memory) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("objectstore: key %q not found", key)
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp, nil
}

// Dir writes blobs under a filesystem prefix (e.g. a mounted PVC).
type Dir struct {
	Root string
}

func (d Dir) Put(_ context.Context, key string, data []byte) error {
	path := filepath.Join(d.Root, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o640)
}

func (d Dir) Get(_ context.Context, key string) ([]byte, error) {
	return os.ReadFile(filepath.Join(d.Root, filepath.FromSlash(key)))
}

// Key builds a stable object key for a workload dump.
func Key(namespace, workloadKey, id string) string {
	w := strings.ReplaceAll(workloadKey, "/", "_")
	return strings.Trim(namespace+"/"+w+"/"+id, "/")
}

// Reader is a helper for io.Reader puts.
func ReadAll(r io.Reader) ([]byte, error) {
	var buf bytes.Buffer
	_, err := buf.ReadFrom(r)
	return buf.Bytes(), err
}
