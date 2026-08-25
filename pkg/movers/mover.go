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

// Package movers is the CSI-shaped plugin surface for data motion.
//
// In-tree implementations (VolSync, CSI snapshot, postgres streaming, …)
// live under pkg/movers/<name>. Out-of-tree movers register the same way.
// Portage never ships a new restic/rsync of its own.
package movers

import (
	"context"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
)

// Capability is what a mover can do for one workload.
type Capability struct {
	Backup    bool
	Replicate bool
	Restore   bool
}

// Artifact is a useful (or not) copy of state.
type Artifact struct {
	ID        string
	Mover     string
	SizeBytes int64
	Useful    bool
	Message   string
}

// ProbeResult is the class-specific "is the data real" check.
// Kubernetes Ready is not sufficient.
type ProbeResult struct {
	OK      bool
	Message string
}

// ClusterHandle is an opaque identifier for a cluster (source or dest).
type ClusterHandle struct {
	Name string
}

// Mover is one data-path plugin. Implementations must be idempotent.
type Mover interface {
	Name() string
	Classes() []portagev1alpha1.WorkloadClass

	Discover(ctx context.Context, w classify.Workload) (Capability, error)

	Backup(ctx context.Context, w classify.Workload, dest ClusterHandle) (Artifact, error)
	Replicate(ctx context.Context, w classify.Workload, src, dst ClusterHandle) error
	Restore(ctx context.Context, w classify.Workload, artifact Artifact, dst ClusterHandle) error

	Quiesce(ctx context.Context, w classify.Workload) error
	Promote(ctx context.Context, w classify.Workload, dst ClusterHandle) error
	Probe(ctx context.Context, w classify.Workload, dst ClusterHandle) (ProbeResult, error)
}

// Registry maps mover name → implementation.
type Registry struct {
	movers []Mover
}

// NewRegistry returns an empty registry. Built-ins are added by the controller/CLI.
func NewRegistry() *Registry {
	return &Registry{}
}

// Register appends a mover. First registered is last in fallback order if
// Select iterates in registration order — callers should register fallbacks last.
func (r *Registry) Register(m Mover) {
	r.movers = append(r.movers, m)
}

// Get returns a mover by name.
func (r *Registry) Get(name string) (Mover, bool) {
	for _, m := range r.movers {
		if m.Name() == name {
			return m, true
		}
	}
	return nil, false
}

// Select picks the first mover that supports the workload class and reports
// capable. override, if set, must exist and is used instead of discovery order.
func (r *Registry) Select(ctx context.Context, w classify.Workload, override string) (Mover, Capability, error) {
	if override != "" {
		m, ok := r.Get(override)
		if !ok {
			return nil, Capability{}, errUnknownMover(override)
		}
		cap, err := m.Discover(ctx, w)
		return m, cap, err
	}
	for _, m := range r.movers {
		if !supports(m, w.Class) {
			continue
		}
		cap, err := m.Discover(ctx, w)
		if err != nil {
			return nil, Capability{}, err
		}
		if cap.Backup || cap.Replicate || cap.Restore {
			return m, cap, nil
		}
	}
	return nil, Capability{}, nil
}

func supports(m Mover, class portagev1alpha1.WorkloadClass) bool {
	for _, c := range m.Classes() {
		if c == class {
			return true
		}
	}
	return false
}

type unknownMoverError string

func (e unknownMoverError) Error() string { return "unknown mover: " + string(e) }

func errUnknownMover(name string) error { return unknownMoverError(name) }
