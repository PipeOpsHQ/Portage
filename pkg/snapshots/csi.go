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

// Package snapshots talks to CSI VolumeSnapshot objects via unstructured
// APIs so Portage does not import the snapshot client.
package snapshots

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/PipeOpsHQ/portage/pkg/classify"
)

var (
	snapshotGVR = schema.GroupVersionResource{
		Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots",
	}
	snapshotClassGVR = schema.GroupVersionResource{
		Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotclasses",
	}
)

const (
	labelName      = "portage.io/name"
	labelNamespace = "portage.io/namespace"
	labelKind      = "portage.io/kind"
	labelPVC       = "portage.io/pvc"
)

// Client wraps a dynamic client.
type Client struct {
	Dynamic dynamic.Interface
}

// CreateForWorkload snapshots each PVC. snapshotClass may be empty (CSI default).
// Returns the number of snapshots created or already present. Missing CRD is not an error.
func (c Client) CreateForWorkload(ctx context.Context, w classify.Workload, snapshotClass string, now time.Time) (int, error) {
	if c.Dynamic == nil || len(w.PVCNames) == 0 {
		return 0, nil
	}
	if snapshotClass == "" {
		snapshotClass = c.defaultClass(ctx)
	}
	n := 0
	for _, pvc := range w.PVCNames {
		name := snapshotName(w, pvc, now)
		obj := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "snapshot.storage.k8s.io/v1",
			"kind":       "VolumeSnapshot",
			"metadata": map[string]any{
				"name":      name,
				"namespace": w.Namespace,
				"labels": map[string]any{
					labelName:      w.Name,
					labelNamespace: w.Namespace,
					labelKind:      w.Kind,
					labelPVC:       pvc,
				},
			},
			"spec": map[string]any{
				"source": map[string]any{
					"persistentVolumeClaimName": pvc,
				},
			},
		}}
		if snapshotClass != "" {
			_ = unstructured.SetNestedField(obj.Object, snapshotClass, "spec", "volumeSnapshotClassName")
		}
		_, err := c.Dynamic.Resource(snapshotGVR).Namespace(w.Namespace).Create(ctx, obj, metav1.CreateOptions{})
		if errors.IsAlreadyExists(err) {
			n++
			continue
		}
		if errors.IsNotFound(err) {
			return n, nil // CRD not installed
		}
		if err != nil {
			return n, fmt.Errorf("create VolumeSnapshot %s/%s: %w", w.Namespace, name, err)
		}
		n++
	}
	return n, nil
}

// LatestReady reports whether any snapshot for the workload is ReadyToUse.
func (c Client) LatestReady(ctx context.Context, w classify.Workload) (bool, string, error) {
	if c.Dynamic == nil {
		return false, "", nil
	}
	list, err := c.Dynamic.Resource(snapshotGVR).Namespace(w.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s", labelName, w.Name, labelKind, w.Kind),
	})
	if err != nil {
		if errors.IsNotFound(err) {
			return false, "", nil
		}
		return false, "", err
	}
	for i := range list.Items {
		ready, _, _ := unstructured.NestedBool(list.Items[i].Object, "status", "readyToUse")
		if ready {
			return true, list.Items[i].GetName(), nil
		}
	}
	if len(list.Items) > 0 {
		return false, list.Items[0].GetName(), nil
	}
	return false, "", nil
}

func (c Client) defaultClass(ctx context.Context) string {
	list, err := c.Dynamic.Resource(snapshotClassGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return ""
	}
	for i := range list.Items {
		ann := list.Items[i].GetAnnotations()
		if ann["snapshot.storage.kubernetes.io/is-default-class"] == "true" {
			return list.Items[i].GetName()
		}
	}
	return ""
}

func snapshotName(w classify.Workload, pvc string, now time.Time) string {
	base := "portage-" + w.Name + "-" + pvc + "-" + now.UTC().Format("20060102-1504")
	base = strings.ToLower(base)
	if len(base) > 63 {
		base = base[:63]
		base = strings.TrimRight(base, "-")
	}
	return base
}
