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
	"encoding/json"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type wireItem struct {
	Group      string         `json:"group"`
	Version    string         `json:"version"`
	Resource   string         `json:"resource"`
	Namespaced bool           `json:"namespaced"`
	Object     map[string]any `json:"object"`
}

// Marshal stores a portable object-graph snapshot (source-death restore).
func Marshal(items []Item) ([]byte, error) {
	wire := make([]wireItem, 0, len(items))
	for _, it := range items {
		if it.Obj == nil {
			continue
		}
		wire = append(wire, wireItem{
			Group: it.GVR.Group, Version: it.GVR.Version, Resource: it.GVR.Resource,
			Namespaced: it.Namespaced, Object: it.Obj.UnstructuredContent(),
		})
	}
	return json.Marshal(wire)
}

// Unmarshal loads a snapshot taken by Marshal.
func Unmarshal(raw []byte) ([]Item, error) {
	var wire []wireItem
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, err
	}
	out := make([]Item, 0, len(wire))
	for _, w := range wire {
		obj := &unstructured.Unstructured{Object: w.Object}
		ns := w.Namespaced
		if !ns && obj.GetNamespace() != "" && w.Resource != "namespaces" {
			ns = true
		}
		out = append(out, Item{
			GVR:        schema.GroupVersionResource{Group: w.Group, Version: w.Version, Resource: w.Resource},
			Namespaced: ns,
			Obj:        obj,
		})
	}
	return out, nil
}
