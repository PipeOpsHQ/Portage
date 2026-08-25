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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/movers"
)

func TestReplicateObjectStoreUsesRclone(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	m := Mover{Dynamic: dyn, Transport: portagev1alpha1.TransportObjectStore, DestPath: "s3://bucket/ns/pg"}
	w := classify.Workload{Namespace: "ns", Name: "pg", Kind: "StatefulSet", PVCNames: []string{"data-pg"}, Class: portagev1alpha1.ClassSQLLogical}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	obj, err := dyn.Resource(srcGVR).Namespace("ns").Get(context.Background(), "portage-pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	path, _, _ := unstructured.NestedString(obj.Object, "spec", "rclone", "rcloneDestPath")
	if path != "s3://bucket/ns/pg" {
		t.Fatalf("rclone path=%q", path)
	}
}

func TestReplicateDirectUsesRsyncTLS(t *testing.T) {
	t.Parallel()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		srcGVR: "ReplicationSourceList",
		dstGVR: "ReplicationDestinationList",
	})
	m := Mover{Dynamic: dyn, Transport: portagev1alpha1.TransportDirect}
	w := classify.Workload{Namespace: "ns", Name: "pg", Kind: "StatefulSet", PVCNames: []string{"data-pg"}}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{}, movers.ClusterHandle{}); err != nil {
		t.Fatal(err)
	}
	obj, err := dyn.Resource(srcGVR).Namespace("ns").Get(context.Background(), "portage-pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, found, _ := unstructured.NestedFieldNoCopy(obj.Object, "spec", "rsyncTLS")
	if !found {
		t.Fatal("expected rsyncTLS")
	}
}
