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
	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/transform"
)

// For returns the renderer selected by Policy.spec.renderer.
// Unknown or empty Kind is Sanitize. Git/Webhook output is still
// passed through transform (defense in depth).
func For(pol *portagev1alpha1.Policy, opt transform.Options) Renderer {
	if pol == nil {
		return Sanitize{Options: opt}
	}
	switch pol.Spec.Renderer.Kind {
	case portagev1alpha1.RendererGit:
		return Git{Source: pol.Spec.Renderer.Git, Options: opt}
	case portagev1alpha1.RendererWebhook:
		return Webhook{URL: pol.Spec.Renderer.WebhookURL, Options: opt}
	default:
		return Sanitize{Options: opt}
	}
}
