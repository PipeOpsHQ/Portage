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

package volsync

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
)

func lagPair(t *testing.T, srcSync, dstSync string, paused bool) Mover {
	t.Helper()
	kinds := map[schema.GroupVersionResource]string{srcGVR: "ReplicationSourceList", dstGVR: "ReplicationDestinationList"}
	obj := func(kind, sync string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "volsync.backube/v1alpha1", "kind": kind,
			"metadata": map[string]any{"name": "portage-pg", "namespace": "ns"},
			"spec":     map[string]any{},
		}}
		if sync != "" {
			_ = unstructured.SetNestedField(u.Object, sync, "status", "lastSyncTime")
		}
		return u
	}
	dst := obj("ReplicationDestination", dstSync)
	if paused {
		_ = unstructured.SetNestedField(dst.Object, true, "spec", "paused")
	}
	return Mover{
		Dynamic:     dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds, obj("ReplicationSource", srcSync)),
		DestDynamic: dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds, dst),
		Transport:   portagev1alpha1.TransportObjectStore,
		DestPath:    "s3://b/p",
	}
}

var lagW = classify.Workload{Namespace: "ns", Name: "pg"}

func TestCutoverLagDrillPausedDestIsBlocked(t *testing.T) {
	t.Parallel()
	// v0.2.16 drill: dest frozen at the earlier Restore, source advancing.
	m := lagPair(t, "2026-11-04T23:55:19Z", "2026-11-04T19:45:25Z", true)
	got, err := m.CutoverLag(t.Context(), lagW, time.Date(2026, 11, 4, 20, 31, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if got.Zero || !got.Blocked || !strings.Contains(got.Reason, "paused") {
		t.Fatalf("%+v", got)
	}
}

func TestCutoverLagDestTrailsSource(t *testing.T) {
	t.Parallel()
	m := lagPair(t, "2026-11-04T23:55:19Z", "2026-11-04T19:45:25Z", false)
	got, err := m.CutoverLag(t.Context(), lagW, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Zero || got.Blocked || !strings.Contains(got.Reason, "dest trails source") {
		t.Fatalf("%+v", got)
	}
}

func TestCutoverLagNeedsSyncAfterFreeze(t *testing.T) {
	t.Parallel()
	frozen := time.Date(2026, 11, 4, 20, 31, 0, 0, time.UTC)
	m := lagPair(t, "2026-11-04T20:30:00Z", "2026-11-04T20:30:30Z", false)
	got, err := m.CutoverLag(t.Context(), lagW, frozen)
	if err != nil {
		t.Fatal(err)
	}
	if got.Zero || !strings.Contains(got.Reason, "source sync after the freeze") {
		t.Fatalf("%+v", got)
	}
	m = lagPair(t, "2026-11-04T20:35:00Z", "2026-11-04T20:30:30Z", false)
	if got, _ = m.CutoverLag(t.Context(), lagW, frozen); got.Zero || !strings.Contains(got.Reason, "dest sync after the freeze") {
		t.Fatalf("%+v", got)
	}
}

func TestCutoverLagZeroAfterPostFreezeSync(t *testing.T) {
	t.Parallel()
	m := lagPair(t, "2026-11-04T20:35:00Z", "2026-11-04T20:36:00Z", false)
	got, err := m.CutoverLag(t.Context(), lagW, time.Date(2026, 11, 4, 20, 31, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Zero {
		t.Fatalf("%+v", got)
	}
}

func TestCutoverLagMissingDest(t *testing.T) {
	t.Parallel()
	kinds := map[schema.GroupVersionResource]string{srcGVR: "ReplicationSourceList", dstGVR: "ReplicationDestinationList"}
	m := Mover{
		Dynamic:     dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds),
		DestDynamic: dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds),
	}
	got, err := m.CutoverLag(t.Context(), lagW, time.Time{})
	if err != nil || got.Zero {
		t.Fatalf("%+v %v", got, err)
	}
}
