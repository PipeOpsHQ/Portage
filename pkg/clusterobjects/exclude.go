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
		p := parseExclude(raw)
		if p.matchGVR(ref, kind) {
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
			return excludePat{crdName: s, resource: strings.SplitN(s, ".", 2)[0], group: strings.SplitN(s, ".", 2)[1]}
		}
		return excludePat{resource: s}
	}
}

func (p excludePat) matchGVR(ref Ref, kind string) bool {
	if p.crdName != "" && ref.Group != "" && ref.Resource+"."+ref.Group == p.crdName {
		return true
	}
	if p.resource != "" && p.group == "" && strings.EqualFold(ref.Resource, p.resource) {
		return true
	}
	if p.resource != "" && p.group != "" && ref.Resource == p.resource && ref.Group == p.group {
		return true
	}
	if p.group != "" && ref.Group != p.group {
		return false
	}
	if p.version != "" && ref.Version != p.version {
		return false
	}
	if p.kind != "" && !strings.EqualFold(kind, p.kind) {
		return false
	}
	if p.group != "" && p.kind != "" {
		return true
	}
	return false
}

func (p excludePat) matchCRD(obj *unstructured.Unstructured) bool {
	name := obj.GetName()
	if p.crdName != "" && name == p.crdName {
		return true
	}
	if p.resource != "" && p.group != "" && name == p.resource+"."+p.group {
		return true
	}
	g, _, _ := unstructured.NestedString(obj.Object, "spec", "group")
	k, _, _ := unstructured.NestedString(obj.Object, "spec", "names", "kind")
	plural, _, _ := unstructured.NestedString(obj.Object, "spec", "names", "plural")
	if p.kind != "" && p.group != "" && strings.EqualFold(k, p.kind) && g == p.group {
		return true
	}
	if p.resource != "" && p.group == "" && strings.EqualFold(plural, p.resource) {
		return true
	}
	return false
}
