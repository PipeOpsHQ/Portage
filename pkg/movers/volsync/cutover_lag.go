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
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/PipeOpsHQ/portage/pkg/classify"
)

// Lag is the Cutover view of one VolSync pair. LagZero only says both sides
// have synced at some point. That is enough for the Replicate live loop, but
// not for promoting dest: in the v0.2.16 drill the dest was paused (a live
// pod mounted the PVC) and its lastSyncTime was two days old, and LagZero
// still returned true.
type Lag struct {
	// Zero is true only when a sync that started after the freeze has
	// landed on dest.
	Zero bool
	// Blocked is true when dest cannot catch up without operator action
	// (ReplicationDestination paused). Waiting longer will not help.
	Blocked bool
	Reason  string
}

// CutoverLag reports whether dest holds every write made before frozenAt.
// Rules:
//   - ReplicationDestination spec.paused is Blocked. Replicate pauses it
//     while a Running dest pod mounts the PVC, and no new bytes arrive.
//   - Source lastSyncTime must be after frozenAt. An earlier sync misses
//     writes made between it and the freeze.
//   - Dest lastSyncTime must be after frozenAt, and with restic (dest
//     restores what source backed up) not before source lastSyncTime.
//
// A zero frozenAt (an Action that predates the SourceFrozen condition) skips
// the freeze comparison but keeps the paused and dest-trails-source checks.
func (m Mover) CutoverLag(ctx context.Context, w classify.Workload, frozenAt time.Time) (Lag, error) {
	name := "portage-" + w.Name
	src, err := getOptional(ctx, m.Dynamic, srcGVR, w.Namespace, name)
	if err != nil {
		return Lag{}, err
	}
	dst, err := getOptional(ctx, m.destDyn(), dstGVR, w.Namespace, name)
	if err != nil {
		return Lag{}, err
	}
	if src == nil {
		return Lag{Reason: fmt.Sprintf("ReplicationSource %s/%s not found", w.Namespace, name)}, nil
	}
	if dst == nil {
		return Lag{Reason: fmt.Sprintf("ReplicationDestination %s/%s not found on dest", w.Namespace, name)}, nil
	}
	dstTime, dstOK := lastSync(dst)
	if paused, _, _ := unstructured.NestedBool(dst.Object, "spec", "paused"); paused {
		at := "never"
		if dstOK {
			at = dstTime.UTC().Format(time.RFC3339)
		}
		return Lag{Blocked: true, Reason: fmt.Sprintf(
			"ReplicationDestination %s/%s is paused because a running dest pod mounts its PVC; dest data stops at lastSyncTime=%s. Scale the dest workload to 0 (or cutover into a dest that has never run a pod) and retry",
			w.Namespace, name, at)}, nil
	}
	srcTime, srcOK := lastSync(src)
	if !srcOK {
		return Lag{Reason: fmt.Sprintf("ReplicationSource %s has no lastSyncTime", name)}, nil
	}
	if !frozenAt.IsZero() && !srcTime.After(frozenAt) {
		return Lag{Reason: fmt.Sprintf("waiting for a source sync after the freeze (source lastSyncTime=%s, frozen at %s)",
			srcTime.UTC().Format(time.RFC3339), frozenAt.UTC().Format(time.RFC3339))}, nil
	}
	if !dstOK {
		return Lag{Reason: fmt.Sprintf("ReplicationDestination %s has no lastSyncTime", name)}, nil
	}
	if !frozenAt.IsZero() && !dstTime.After(frozenAt) {
		return Lag{Reason: fmt.Sprintf("waiting for dest sync after the freeze (dest lastSyncTime=%s, frozen at %s)",
			dstTime.UTC().Format(time.RFC3339), frozenAt.UTC().Format(time.RFC3339))}, nil
	}
	if m.resticCopy() && dstTime.Before(srcTime) {
		return Lag{Reason: fmt.Sprintf("dest trails source (dest lastSyncTime=%s, source lastSyncTime=%s)",
			dstTime.UTC().Format(time.RFC3339), srcTime.UTC().Format(time.RFC3339))}, nil
	}
	return Lag{Zero: true, Reason: fmt.Sprintf("dest synced at %s", dstTime.UTC().Format(time.RFC3339))}, nil
}

// getOptional returns nil (no error) when the object does not exist.
func getOptional(ctx context.Context, dyn dynamic.Interface, gvr schema.GroupVersionResource, ns, name string) (*unstructured.Unstructured, error) {
	if dyn == nil {
		return nil, fmt.Errorf("volsync: %s client required", gvr.Resource)
	}
	obj, err := dyn.Resource(gvr).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("volsync %s %s/%s: %w", gvr.Resource, ns, name, err)
	}
	return obj, nil
}

func lastSync(obj *unstructured.Unstructured) (time.Time, bool) {
	s, found, _ := unstructured.NestedString(obj.Object, "status", "lastSyncTime")
	if !found || s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
