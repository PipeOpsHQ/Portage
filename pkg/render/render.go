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

// Package render produces destination manifests. The default is Sanitize
// (clone live objects, strip cluster-local fields). A PaaS or GitOps engine
// implements Renderer out of tree and is selected via Policy.spec.renderer.
package render

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/transform"
)

// Request is dest-side rendering input.
type Request struct {
	Policy        *portagev1alpha1.Policy
	Pair          *portagev1alpha1.ClusterPair
	SourceObjects []*unstructured.Unstructured
}

// Renderer produces dest objects. Implementations must not return source
// UIDs, volumeHandles, or zone pins.
type Renderer interface {
	Kind() portagev1alpha1.RendererKind
	Render(ctx context.Context, req Request) ([]*unstructured.Unstructured, error)
}

// Sanitize is the built-in renderer: clone + strip.
type Sanitize struct {
	Options transform.Options
}

// Kind implements Renderer.
func (Sanitize) Kind() portagev1alpha1.RendererKind {
	return portagev1alpha1.RendererSanitize
}

// Render implements Renderer.
func (s Sanitize) Render(_ context.Context, req Request) ([]*unstructured.Unstructured, error) {
	out := make([]*unstructured.Unstructured, 0, len(req.SourceObjects))
	opt := s.Options
	if req.Pair != nil && opt.StorageClassMap == nil {
		opt.StorageClassMap = req.Pair.Spec.StorageClassMap
	}
	for _, src := range req.SourceObjects {
		cp := src.DeepCopy()
		transform.Object(cp, opt)
		out = append(out, cp)
	}
	return out, nil
}
