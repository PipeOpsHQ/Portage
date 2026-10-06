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

package clusterobjects

import (
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func excludedGVR(ref Ref, kind string, exclude []string) bool {
	for _, raw := range exclude {
		if parseExclude(raw).matchGVR(ref, kind) {
			return true
		}
	}
	return false
}

func excludedObj(gvr schema.GroupVersionResource, obj *unstructured.Unstructured, exclude []string) bool {
	if obj == nil {
		return false
	}
	for _, raw := range exclude {
		p := parseExclude(raw)
		if p.matchGVR(Ref{GroupVersionResource: gvr}, obj.GetKind()) {
			return true
		}
		if gvr.Resource == "customresourcedefinitions" && p.matchCRD(obj) {
			return true
		}
	}
	return false
}

type excludePat struct {
	group, version, kind, resource, crdName string
}

func parseExclude(s string) excludePat {
	s = strings.TrimSpace(s)
	parts := strings.Split(s, "/")
	switch len(parts) {
	case 3:
		return excludePat{group: parts[0], version: parts[1], kind: parts[2]}
	case 2:
		return excludePat{group: parts[0], kind: parts[1]}
	default:
		if strings.Contains(s, ".") {
			res, group, _ := strings.Cut(s, ".")
			return excludePat{crdName: s, resource: res, group: group}
		}
		return excludePat{resource: s}
	}
}

func (p excludePat) matchGVR(ref Ref, kind string) bool {
	if p.crdName != "" && ref.Group != "" {
		if strings.EqualFold(ref.Resource+"."+ref.Group, p.crdName) {
			return true
		}
		if strings.EqualFold(ref.Resource, p.resource) && strings.EqualFold(ref.Group, p.group) {
			return true
		}
	}
	if p.resource != "" && p.group == "" && strings.EqualFold(ref.Resource, p.resource) {
		return true
	}
	if p.resource != "" && p.group != "" && p.kind == "" && p.crdName == "" &&
		strings.EqualFold(ref.Resource, p.resource) && strings.EqualFold(ref.Group, p.group) {
		return true
	}
	if p.group == "" || p.kind == "" {
		return false
	}
	if !strings.EqualFold(ref.Group, p.group) {
		return false
	}
	if p.version != "" && !strings.EqualFold(ref.Version, p.version) {
		return false
	}
	if kind != "" && strings.EqualFold(kind, p.kind) {
		return true
	}
	return kindMatchesResource(p.kind, ref.Resource)
}

func (p excludePat) matchCRD(obj *unstructured.Unstructured) bool {
	name := obj.GetName()
	if p.crdName != "" && strings.EqualFold(name, p.crdName) {
		return true
	}
	if p.resource != "" && p.group != "" && strings.EqualFold(name, p.resource+"."+p.group) {
		return true
	}
	g, _, _ := unstructured.NestedString(obj.Object, "spec", "group")
	k, _, _ := unstructured.NestedString(obj.Object, "spec", "names", "kind")
	plural, _, _ := unstructured.NestedString(obj.Object, "spec", "names", "plural")
	if p.kind != "" && p.group != "" {
		if strings.EqualFold(g, p.group) && strings.EqualFold(k, p.kind) {
			return true
		}
		if strings.EqualFold(g, p.group) && kindMatchesResource(p.kind, plural) {
			return true
		}
		if i := strings.Index(name, "."); i > 0 &&
			strings.EqualFold(name[i+1:], p.group) && kindMatchesResource(p.kind, name[:i]) {
			return true
		}
	}
	if p.resource != "" && p.group == "" && strings.EqualFold(plural, p.resource) {
		return true
	}
	if p.crdName != "" && g != "" && plural != "" && strings.EqualFold(plural+"."+g, p.crdName) {
		return true
	}
	return false
}

// kindMatchesResource maps Kind to the usual plural resource name so
// group/kind still matches when discovery omits APIResource.Kind
// (GatewayClass → gatewayclasses, BackendTLSPolicy → backendtlspolicies).
func kindMatchesResource(kind, resource string) bool {
	if kind == "" || resource == "" {
		return false
	}
	k, r := strings.ToLower(kind), strings.ToLower(resource)
	if k == r || k+"s" == r || k+"es" == r {
		return true
	}
	if strings.HasSuffix(k, "y") && k[:len(k)-1]+"ies" == r {
		return true
	}
	if strings.HasSuffix(k, "ss") && k+"es" == r {
		return true
	}
	if strings.HasSuffix(k, "class") && strings.TrimSuffix(k, "class")+"classes" == r {
		return true
	}
	return false
}
