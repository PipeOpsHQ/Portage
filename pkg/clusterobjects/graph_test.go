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
	"context"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakediscovery "k8s.io/client-go/discovery/fake"
	dynfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/transform"
)

func TestSyncThenAttestConfigMap(t *testing.T) {
	t.Parallel()
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	src := dynfake.NewSimpleDynamicClient(scheme, &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{Kind: "ConfigMap", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns", UID: "src-uid"},
		Data:       map[string]string{"k": "v"},
	})
	dst := dynfake.NewSimpleDynamicClient(scheme)

	items, err := List(context.Background(), src, []Ref{{GroupVersionResource: gvr, Namespaced: true}}, []string{"ns"}, portagev1alpha1.ClusterObjectsSpec{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("listed %d", len(items))
	}
	items = Sanitize(items, transform.Options{})
	if items[0].Obj.GetUID() != "" {
		t.Fatal("sanitize must drop UID")
	}
	if err := Sync(context.Background(), dst, items); err != nil {
		t.Fatal(err)
	}
	ok, msg := Attest(context.Background(), dst, items)
	if !ok {
		t.Fatal(msg)
	}
	got, err := dst.Resource(gvr).Namespace("ns").Get(context.Background(), "app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Object["data"].(map[string]any)["k"] != "v" {
		t.Fatalf("%v", got.Object)
	}
}

func TestActiveReplicationUpdatesDest(t *testing.T) {
	t.Parallel()
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	cm := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{Kind: "ConfigMap", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns"},
		Data:       map[string]string{"k": "v1"},
	}
	src := dynfake.NewSimpleDynamicClient(scheme, cm.DeepCopy())
	dst := dynfake.NewSimpleDynamicClient(scheme)
	items, _ := List(context.Background(), src, []Ref{{GroupVersionResource: gvr, Namespaced: true}}, []string{"ns"}, portagev1alpha1.ClusterObjectsSpec{})
	_ = Sync(context.Background(), dst, Sanitize(items, transform.Options{}))

	cm.Data["k"] = "v2"
	src = dynfake.NewSimpleDynamicClient(scheme, cm)
	items, _ = List(context.Background(), src, []Ref{{GroupVersionResource: gvr, Namespaced: true}}, []string{"ns"}, portagev1alpha1.ClusterObjectsSpec{})
	if err := Sync(context.Background(), dst, Sanitize(items, transform.Options{})); err != nil {
		t.Fatal(err)
	}
	got, err := dst.Resource(gvr).Namespace("ns").Get(context.Background(), "app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Object["data"].(map[string]any)["k"] != "v2" {
		t.Fatalf("dest not live-synced: %v", got.Object["data"])
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	t.Parallel()
	items := []Item{{
		GVR: schema.GroupVersionResource{Version: "v1", Resource: "secrets"},
		Obj: &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Secret",
			"metadata": map[string]any{"name": "s", "namespace": "ns"},
		}},
	}}
	raw, err := Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	back, err := Unmarshal(raw)
	if err != nil || len(back) != 1 || back[0].Obj.GetName() != "s" {
		t.Fatalf("%v %v", err, back)
	}
}

func TestAttestFailsWhenDestMissing(t *testing.T) {
	t.Parallel()
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	dst := dynfake.NewSimpleDynamicClient(scheme)
	items := []Item{{
		GVR: gvr,
		Obj: &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "app", "namespace": "ns"},
		}},
	}}
	ok, msg := Attest(context.Background(), dst, items)
	if ok {
		t.Fatal("dest Get miss must not attest")
	}
	if msg == "" {
		t.Fatal("expected missing-object message")
	}
}

func TestDiscoverSkipsEphemeralKeepsUnknownCR(t *testing.T) {
	t.Parallel()
	disco := &fakediscovery.FakeDiscovery{Fake: &clienttesting.Fake{
		Resources: []*metav1.APIResourceList{
			{GroupVersion: "v1", APIResources: []metav1.APIResource{
				{Name: "pods", Namespaced: true, Kind: "Pod", Verbs: []string{"list", "create", "get"}},
				{Name: "configmaps", Namespaced: true, Kind: "ConfigMap", Verbs: []string{"list", "create", "get"}},
				{Name: "nodes", Namespaced: false, Kind: "Node", Verbs: []string{"list", "create"}},
				{Name: "namespaces", Namespaced: false, Kind: "Namespace", Verbs: []string{"list", "create"}},
			}},
			{GroupVersion: "networking.k8s.io/v1", APIResources: []metav1.APIResource{
				{Name: "servicecidrs", Namespaced: false, Kind: "ServiceCIDR", Verbs: []string{"list", "create", "get"}},
				{Name: "ipaddresses", Namespaced: false, Kind: "IPAddress", Verbs: []string{"list", "create", "get"}},
				{Name: "networkpolicies", Namespaced: true, Kind: "NetworkPolicy", Verbs: []string{"list", "create", "get"}},
			}},
			{GroupVersion: "apiextensions.k8s.io/v1", APIResources: []metav1.APIResource{
				{Name: "customresourcedefinitions", Namespaced: false, Kind: "CustomResourceDefinition", Verbs: []string{"list", "create", "get"}},
			}},
			{GroupVersion: "stable.example.com/v1", APIResources: []metav1.APIResource{
				{Name: "widgets", Namespaced: true, Kind: "Widget", Verbs: []string{"list", "create", "get"}},
				{Name: "clusterwidgets", Namespaced: false, Kind: "ClusterWidget", Verbs: []string{"list", "create", "get"}},
			}},
			{GroupVersion: "events.k8s.io/v1", APIResources: []metav1.APIResource{
				{Name: "events", Namespaced: true, Kind: "Event", Verbs: []string{"list", "create"}},
			}},
			{GroupVersion: "volsync.backube/v1alpha1", APIResources: []metav1.APIResource{
				{Name: "replicationsources", Namespaced: true, Kind: "ReplicationSource", Verbs: []string{"list", "create", "get"}},
			}},
			{GroupVersion: "k8up.io/v1", APIResources: []metav1.APIResource{
				{Name: "schedules", Namespaced: true, Kind: "Schedule", Verbs: []string{"list", "create", "get"}},
			}},
			{GroupVersion: "velero.io/v1", APIResources: []metav1.APIResource{
				{Name: "backups", Namespaced: true, Kind: "Backup", Verbs: []string{"list", "create", "get"}},
			}},
			{GroupVersion: "stash.appscode.com/v1beta1", APIResources: []metav1.APIResource{
				{Name: "backupconfigurations", Namespaced: true, Kind: "BackupConfiguration", Verbs: []string{"list", "create", "get"}},
			}},
			{GroupVersion: "capsule.clastix.io/v1beta2", APIResources: []metav1.APIResource{
				{Name: "capsuleconfigurations", Namespaced: false, Kind: "CapsuleConfiguration", Verbs: []string{"list", "create", "get"}},
			}},
		},
	}}
	off := false
	gvrs, err := Discover(disco, portagev1alpha1.ClusterObjectsSpec{IncludeClusterScoped: &off})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, g := range gvrs {
		got[g.Resource] = true
	}
	if got["pods"] || got["nodes"] || got["events"] {
		t.Fatalf("ephemeral leaked: %v", gvrs)
	}
	if !got["configmaps"] {
		t.Fatal("configmaps must stay in the graph")
	}
	if !got["widgets"] {
		t.Fatal("unknown CR must stay in the graph")
	}
	if !got["customresourcedefinitions"] {
		t.Fatal("CRDs must always be in the graph")
	}
	if got["namespaces"] || got["clusterwidgets"] {
		t.Fatalf("cluster-scoped except CRDs require IncludeClusterScoped: %v", gvrs)
	}
	scoped, err := Discover(disco, portagev1alpha1.ClusterObjectsSpec{})
	if err != nil {
		t.Fatal(err)
	}
	var ns, crd, clusterCR, np bool
	for _, g := range scoped {
		if g.Resource == "namespaces" {
			ns = true
		}
		if g.Resource == "customresourcedefinitions" {
			crd = true
		}
		if g.Resource == "clusterwidgets" {
			clusterCR = true
		}
		if g.Resource == "nodes" {
			t.Fatal("nodes are dest-local")
		}
		if g.Resource == "servicecidrs" || g.Resource == "ipaddresses" {
			t.Fatal("ServiceCIDR/IPAddress are dest-local (immutable cluster CIDR)")
		}
		if g.Resource == "replicationsources" || g.Resource == "capsuleconfigurations" {
			t.Fatal("VolSync CRs and CapsuleConfiguration are dest-local")
		}
		if g.Resource == "schedules" || g.Resource == "backups" || g.Resource == "backupconfigurations" {
			t.Fatal("dest-local backup CRs (K8up/Velero/Stash) must not be copied")
		}
		if g.Resource == "networkpolicies" {
			np = true
		}
	}
	if !ns {
		t.Fatal("IncludeClusterScoped must add namespaces")
	}
	if !crd {
		t.Fatal("CRDs must stay when IncludeClusterScoped")
	}
	if !clusterCR {
		t.Fatal("unknown cluster-scoped CR must stay in the graph")
	}
	if !np {
		t.Fatal("namespaced NetworkPolicy must stay in the graph")
	}
}

func TestForbiddenGVRSkipped(t *testing.T) {
	t.Parallel()
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	src := dynfake.NewSimpleDynamicClient(scheme)
	src.PrependReactor("list", "secrets", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "", fmt.Errorf("no"))
	})
	items, err := List(context.Background(), src, []Ref{{GroupVersionResource: gvr, Namespaced: true}}, []string{"ns"}, portagev1alpha1.ClusterObjectsSpec{})
	if err != nil {
		t.Fatalf("403 must skip, not fail: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("listed %d", len(items))
	}
}

func TestSkipDestLocalObjects(t *testing.T) {
	t.Parallel()
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	src := dynfake.NewSimpleDynamicClient(scheme, &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{Kind: "Secret", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "default-token", Namespace: "ns"},
		Type:       corev1.SecretTypeServiceAccountToken,
	}, &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{Kind: "Secret", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns"},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"k": []byte("v")},
	})
	items, err := List(context.Background(), src, []Ref{{GroupVersionResource: gvr, Namespaced: true}}, []string{"ns"}, portagev1alpha1.ClusterObjectsSpec{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Obj.GetName() != "app" {
		t.Fatalf("SA tokens are dest-local: %+v", items)
	}
}

func TestCRDAttestRequiresEstablished(t *testing.T) {
	t.Parallel()
	gvr := schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	crd := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": "widgets.stable.example.com"},
	}}
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		gvr: "CustomResourceDefinitionList",
	}, crd)
	items := []Item{{GVR: gvr, Obj: crd}}
	ok, _ := Attest(context.Background(), dyn, items)
	if ok {
		t.Fatal("CRD without Established must not attest")
	}
	_ = unstructured.SetNestedSlice(crd.Object, []any{
		map[string]any{"type": "Established", "status": "True"},
	}, "status", "conditions")
	dyn = dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		gvr: "CustomResourceDefinitionList",
	}, crd)
	ok, msg := Attest(context.Background(), dyn, items)
	if !ok {
		t.Fatal(msg)
	}
}

func TestUnknownCRStaysInGraphAndLiveSyncs(t *testing.T) {
	t.Parallel()
	gvr := schema.GroupVersionResource{Group: "stable.example.com", Version: "v1", Resource: "widgets"}
	widget := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "stable.example.com/v1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": "w1", "namespace": "ns", "uid": "src"},
		"spec":       map[string]any{"n": "v1"},
	}}
	lists := map[schema.GroupVersionResource]string{gvr: "WidgetList"}
	src := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), lists, widget)
	dst := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), lists)
	items, err := List(context.Background(), src, []Ref{{GroupVersionResource: gvr, Namespaced: true}}, []string{"ns"}, portagev1alpha1.ClusterObjectsSpec{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("unknown CR dropped: %d", len(items))
	}
	if err := Sync(context.Background(), dst, Sanitize(items, transform.Options{})); err != nil {
		t.Fatal(err)
	}
	ok, msg := Attest(context.Background(), dst, items)
	if !ok {
		t.Fatal(msg)
	}
	got, err := dst.Resource(gvr).Namespace("ns").Get(context.Background(), "w1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetUID() == "src" {
		t.Fatal("sanitize must drop source UID")
	}
}

func TestCRDThenCRLiveSync(t *testing.T) {
	t.Parallel()
	crdGVR := schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	crGVR := schema.GroupVersionResource{Group: "stable.example.com", Version: "v1", Resource: "widgets"}
	crd := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": "widgets.stable.example.com"},
		"spec":       map[string]any{"group": "stable.example.com"},
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "Established", "status": "True"},
		}},
	}}
	widget := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "stable.example.com/v1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": "w1", "namespace": "ns"},
		"spec":       map[string]any{"k": "v1"},
	}}
	lists := map[schema.GroupVersionResource]string{
		crdGVR: "CustomResourceDefinitionList",
		crGVR:  "WidgetList",
	}
	src := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), lists, crd, widget)
	dst := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), lists)
	items, err := List(context.Background(), src, []Ref{
		{GroupVersionResource: crdGVR, Namespaced: false},
		{GroupVersionResource: crGVR, Namespaced: true},
	}, []string{"ns"}, portagev1alpha1.ClusterObjectsSpec{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("want CRD+CR, got %d", len(items))
	}
	if err := Sync(context.Background(), dst, Sanitize(items, transform.Options{})); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Resource(crdGVR).Get(context.Background(), "widgets.stable.example.com", metav1.GetOptions{}); err != nil {
		t.Fatalf("dest CRD: %v", err)
	}
	got, err := dst.Resource(crGVR).Namespace("ns").Get(context.Background(), "w1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("dest CR: %v", err)
	}
	spec, _, _ := unstructured.NestedString(got.Object, "spec", "k")
	if spec != "v1" {
		t.Fatalf("widget spec.k=%s", spec)
	}
}

func TestSkipPortageCRDAndSystemClusterRole(t *testing.T) {
	t.Parallel()
	crdGVR := schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	roleGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	lists := map[schema.GroupVersionResource]string{
		crdGVR:  "CustomResourceDefinitionList",
		roleGVR: "ClusterRoleList",
	}
	src := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), lists,
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
			"metadata": map[string]any{"name": "policies.portage.io"},
		}},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
			"metadata": map[string]any{"name": "widgets.stable.example.com"},
		}},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
			"metadata": map[string]any{"name": "system:controller:generic-garbage-collector"},
		}},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
			"metadata": map[string]any{"name": "tenant-admin"},
		}},
	)
	items, err := List(context.Background(), src, []Ref{
		{GroupVersionResource: crdGVR, Namespaced: false},
		{GroupVersionResource: roleGVR, Namespaced: false},
	}, []string{"ns"}, portagev1alpha1.ClusterObjectsSpec{})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, it := range items {
		got[it.Obj.GetName()] = true
	}
	if got["policies.portage.io"] {
		t.Fatal("portage CRDs are dest-local")
	}
	if got["system:controller:generic-garbage-collector"] {
		t.Fatal("system ClusterRoles are dest-local")
	}
	if !got["widgets.stable.example.com"] || !got["tenant-admin"] {
		t.Fatalf("user CRD and ClusterRole dropped: %v", got)
	}
}

func TestExcludeAllDocumentedForms(t *testing.T) {
	t.Parallel()
	tlsGVR := Ref{GroupVersionResource: schema.GroupVersionResource{
		Group: "gateway.networking.k8s.io", Version: "v1alpha3", Resource: "backendtlspolicies",
	}}
	gcGVR := Ref{GroupVersionResource: schema.GroupVersionResource{
		Group: "gateway.networking.k8s.io", Version: "v1", Resource: "gatewayclasses",
	}}
	crdGVR := schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	tlsCRD := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
		"metadata": map[string]any{"name": "backendtlspolicies.gateway.networking.k8s.io"},
		"spec":     map[string]any{"group": "gateway.networking.k8s.io", "names": map[string]any{"kind": "BackendTLSPolicy", "plural": "backendtlspolicies"}},
	}}
	gcCRD := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
		"metadata": map[string]any{"name": "gatewayclasses.gateway.networking.k8s.io"},
		"spec":     map[string]any{"group": "gateway.networking.k8s.io", "names": map[string]any{"kind": "GatewayClass", "plural": "gatewayclasses"}},
	}}
	keepCRD := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
		"metadata": map[string]any{"name": "widgets.stable.example.com"},
		"spec":     map[string]any{"group": "stable.example.com", "names": map[string]any{"kind": "Widget", "plural": "widgets"}},
	}}
	forms := []struct {
		exclude string
		tls, gc bool
	}{
		{"backendtlspolicies.gateway.networking.k8s.io", true, false},
		{"gateway.networking.k8s.io/BackendTLSPolicy", true, false},
		{"gateway.networking.k8s.io/v1alpha3/BackendTLSPolicy", true, false},
		{"backendtlspolicies", true, false},
		{"gatewayclasses", false, true},
		{"gateway.networking.k8s.io/GatewayClass", false, true},
		{"gateway.networking.k8s.io/v1/GatewayClass", false, true},
	}
	for _, tc := range forms {
		ex := []string{tc.exclude}
		if excludedGVR(tlsGVR, "BackendTLSPolicy", ex) != tc.tls {
			t.Errorf("%s: backendtlspolicies GVR match=%v want %v", tc.exclude, !tc.tls, tc.tls)
		}
		if excludedGVR(tlsGVR, "", ex) != tc.tls {
			t.Errorf("%s: backendtlspolicies GVR with empty Kind match=%v want %v", tc.exclude, !tc.tls, tc.tls)
		}
		if excludedObj(crdGVR, tlsCRD, ex) != tc.tls {
			t.Errorf("%s: BackendTLSPolicy CRD object match=%v want %v", tc.exclude, !tc.tls, tc.tls)
		}
		if excludedGVR(gcGVR, "GatewayClass", ex) != tc.gc {
			t.Errorf("%s: gatewayclasses GVR match=%v want %v", tc.exclude, !tc.gc, tc.gc)
		}
		if excludedGVR(gcGVR, "", ex) != tc.gc {
			t.Errorf("%s: gatewayclasses GVR with empty Kind match=%v want %v", tc.exclude, !tc.gc, tc.gc)
		}
		if excludedObj(crdGVR, gcCRD, ex) != tc.gc {
			t.Errorf("%s: GatewayClass CRD object match=%v want %v", tc.exclude, !tc.gc, tc.gc)
		}
		if excludedObj(crdGVR, keepCRD, ex) {
			t.Errorf("%s: must not exclude unrelated CRD", tc.exclude)
		}
	}
}

func TestDiscoverExcludeGVKs(t *testing.T) {
	t.Parallel()
	disco := &fakediscovery.FakeDiscovery{Fake: &clienttesting.Fake{
		Resources: []*metav1.APIResourceList{
			{GroupVersion: "gateway.networking.k8s.io/v1alpha3", APIResources: []metav1.APIResource{
				{Name: "backendtlspolicies", Namespaced: true, Kind: "BackendTLSPolicy", Verbs: []string{"list", "create", "get"}},
			}},
			{GroupVersion: "v1", APIResources: []metav1.APIResource{
				{Name: "configmaps", Namespaced: true, Kind: "ConfigMap", Verbs: []string{"list", "create", "get"}},
			}},
		},
	}}
	gvrs, err := Discover(disco, portagev1alpha1.ClusterObjectsSpec{
		ExcludeGVKs: []string{"gateway.networking.k8s.io/BackendTLSPolicy"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range gvrs {
		if g.Resource == "backendtlspolicies" {
			t.Fatal("excluded GVK must not be discovered")
		}
	}
	var cm bool
	for _, g := range gvrs {
		if g.Resource == "configmaps" {
			cm = true
		}
	}
	if !cm {
		t.Fatal("configmaps must stay")
	}
}

func TestListExcludeCRDName(t *testing.T) {
	t.Parallel()
	crdGVR := schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	src := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		crdGVR: "CustomResourceDefinitionList",
	},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
			"metadata": map[string]any{"name": "backendtlspolicies.gateway.networking.k8s.io"},
			"spec":     map[string]any{"group": "gateway.networking.k8s.io", "names": map[string]any{"kind": "BackendTLSPolicy", "plural": "backendtlspolicies"}},
		}},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
			"metadata": map[string]any{"name": "widgets.stable.example.com"},
			"spec":     map[string]any{"group": "stable.example.com", "names": map[string]any{"kind": "Widget", "plural": "widgets"}},
		}},
	)
	items, err := List(context.Background(), src, []Ref{{GroupVersionResource: crdGVR, Namespaced: false}}, nil, portagev1alpha1.ClusterObjectsSpec{
		ExcludeGVKs: []string{"backendtlspolicies.gateway.networking.k8s.io"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Obj.GetName() != "widgets.stable.example.com" {
		t.Fatalf("got %+v", items)
	}
}

func TestSyncRetriesConflict(t *testing.T) {
	t.Parallel()
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	dst := dynfake.NewSimpleDynamicClient(scheme, &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{Kind: "ConfigMap", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns", ResourceVersion: "1"},
		Data:       map[string]string{"k": "old"},
	})
	conflicts := 0
	dst.PrependReactor("update", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		conflicts++
		if conflicts == 1 {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, "app", fmt.Errorf("stale"))
		}
		return false, nil, nil
	})
	items := []Item{{
		GVR: gvr, Namespaced: true,
		Obj: &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "app", "namespace": "ns"},
			"data":     map[string]any{"k": "new"},
		}},
	}}
	if err := Sync(context.Background(), dst, items); err != nil {
		t.Fatal(err)
	}
	if conflicts < 1 {
		t.Fatal("expected a conflict retry")
	}
}

func TestSyncDeletesTerminatingAndKeepsGoing(t *testing.T) {
	t.Parallel()
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	dead := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "dead", Namespace: "ns"}}
	dst := dynfake.NewSimpleDynamicClient(scheme, dead)
	now := metav1.Now()
	items := []Item{
		{
			GVR: gvr, Namespaced: true, Deleting: true,
			Obj: &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": "dead", "namespace": "ns", "deletionTimestamp": now.UTC().Format("2006-01-02T15:04:05Z")},
			}},
		},
		{
			GVR: gvr, Namespaced: true,
			Obj: &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": "live", "namespace": "ns"},
				"data":     map[string]any{"k": "v"},
			}},
		},
	}
	if err := Sync(context.Background(), dst, items); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Resource(gvr).Namespace("ns").Get(context.Background(), "dead", metav1.GetOptions{}); err == nil {
		t.Fatal("terminating source object must be deleted on dest")
	}
	if _, err := dst.Resource(gvr).Namespace("ns").Get(context.Background(), "live", metav1.GetOptions{}); err != nil {
		t.Fatalf("later objects must still apply: %v", err)
	}
}

func TestForbiddenTenantListSurfaces(t *testing.T) {
	t.Parallel()
	gvr := schema.GroupVersionResource{Group: "capsule.clastix.io", Version: "v1beta2", Resource: "tenants"}
	src := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		gvr: "TenantList",
	})
	src.PrependReactor("list", "tenants", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "capsule.clastix.io", Resource: "tenants"}, "", fmt.Errorf("no"))
	})
	_, err := List(context.Background(), src, []Ref{{GroupVersionResource: gvr, Namespaced: false}}, nil, portagev1alpha1.ClusterObjectsSpec{})
	if err == nil || !strings.Contains(err.Error(), "tenants") {
		t.Fatalf("forbidden Capsule Tenant list must surface, got %v", err)
	}
}

func TestSyncCapsuleTenantBeforeNamespace(t *testing.T) {
	t.Parallel()
	dst := dynfake.NewSimpleDynamicClient(runtime.NewScheme())
	var order []string
	dst.PrependReactor("create", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		order = append(order, action.GetResource().Resource)
		return false, nil, nil
	})
	items := []Item{capsuleNamespace("app", "acme"), capsuleTenant("acme")}
	if err := Sync(context.Background(), dst, items); err != nil {
		t.Fatal(err)
	}
	if len(order) < 2 || order[0] != "tenants" || order[1] != "namespaces" {
		t.Fatalf("apply order %v", order)
	}
	got, err := dst.Resource(nsGVR()).Get(context.Background(), "app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetLabels()[TenantLabel] != "acme" {
		t.Fatalf("labels %#v", got.GetLabels())
	}
}

func TestSyncDoesNotAdoptUnownedCapsuleNamespace(t *testing.T) {
	t.Parallel()
	dst := dynfake.NewSimpleDynamicClient(runtime.NewScheme())
	bare := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace",
		"metadata": map[string]any{"name": "app"},
	}}
	if _, err := dst.Resource(nsGVR()).Create(context.Background(), bare, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Resource(tenantGVR()).Create(context.Background(), capsuleTenant("acme").Obj, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	err := Sync(context.Background(), dst, []Item{capsuleNamespace("app", "acme")})
	if err == nil || !strings.Contains(err.Error(), "ownerReference") || !strings.Contains(err.Error(), "delete namespace") {
		t.Fatalf("got %v", err)
	}
	got, getErr := dst.Resource(nsGVR()).Get(context.Background(), "app", metav1.GetOptions{})
	if getErr != nil {
		t.Fatal(getErr)
	}
	if got.GetLabels()[TenantLabel] != "" {
		t.Fatalf("label was patched onto an unowned namespace: %#v", got.GetLabels())
	}
}

func TestSyncKeepsDestCapsuleOwnerReference(t *testing.T) {
	t.Parallel()
	dst := dynfake.NewSimpleDynamicClient(runtime.NewScheme())
	owned := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace",
		"metadata": map[string]any{
			"name":   "app",
			"labels": map[string]any{TenantLabel: "acme"},
			"ownerReferences": []any{map[string]any{
				"apiVersion": "capsule.clastix.io/v1beta2",
				"kind":       "Tenant",
				"name":       "acme",
				"uid":        "dest-uid",
			}},
		},
	}}
	if _, err := dst.Resource(nsGVR()).Create(context.Background(), owned, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Resource(tenantGVR()).Create(context.Background(), capsuleTenant("acme").Obj, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// Sanitize has already dropped the source ownerReference.
	if err := Sync(context.Background(), dst, []Item{capsuleNamespace("app", "acme")}); err != nil {
		t.Fatal(err)
	}
	got, err := dst.Resource(nsGVR()).Get(context.Background(), "app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !HasCapsuleOwner(got) {
		t.Fatalf("dest Tenant ownerReference was stripped: %#v", got.GetOwnerReferences())
	}
}

func TestSyncRefusesNamespaceWithoutDestTenant(t *testing.T) {
	t.Parallel()
	dst := dynfake.NewSimpleDynamicClient(runtime.NewScheme())
	err := Sync(context.Background(), dst, []Item{capsuleNamespace("app", "acme")})
	if err == nil || !strings.Contains(err.Error(), "Tenant \"acme\"") {
		t.Fatalf("got %v", err)
	}
	if _, getErr := dst.Resource(nsGVR()).Get(context.Background(), "app", metav1.GetOptions{}); !apierrors.IsNotFound(getErr) {
		t.Fatalf("namespace must not be created, get err %v", getErr)
	}
}

func TestSyncSurfacesCapsuleAdmissionDenial(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "forbidden deny",
			err: apierrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, "app",
				fmt.Errorf("admission webhook %q denied the request: only tenant owners can create tenant-owned namespaces", "namespaces.validating.projectcapsule.dev")),
			want: "spec.owners",
		},
		{
			name: "bad request non-owned",
			err:  apierrors.NewBadRequest(`admission webhook "namespaces.projectcapsule.dev" denied the request: can not assign the desired namespace to a non-owned Tenant`),
			want: "spec.owners",
		},
		{
			name: "validating webhook missing ownerReference",
			err:  apierrors.NewInternalError(fmt.Errorf("admission webhook %q denied the request: namespace has tenant label %q but no Tenant ownerReference", "namespaces.validating.projectcapsule.dev", "acme")),
			want: "spec.owners",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dst := dynfake.NewSimpleDynamicClient(runtime.NewScheme())
			if _, err := dst.Resource(tenantGVR()).Create(context.Background(), capsuleTenant("acme").Obj, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			dst.PrependReactor("create", "namespaces", func(action clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, tc.err
			})
			err := Sync(context.Background(), dst, []Item{capsuleNamespace("app", "acme")})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v", err)
			}
			if _, getErr := dst.Resource(nsGVR()).Get(context.Background(), "app", metav1.GetOptions{}); !apierrors.IsNotFound(getErr) {
				t.Fatalf("denied create must not leave a namespace, get %v", getErr)
			}
		})
	}
}

func TestSyncFindsTenantOnOlderVersionWhenPreferredIsForbidden(t *testing.T) {
	t.Parallel()
	dst := dynfake.NewSimpleDynamicClient(runtime.NewScheme())
	older := schema.GroupVersionResource{Group: "capsule.clastix.io", Version: "v1beta1", Resource: "tenants"}
	obj := capsuleTenant("acme").Obj.DeepCopy()
	obj.SetAPIVersion("capsule.clastix.io/v1beta1")
	if _, err := dst.Resource(older).Create(context.Background(), obj, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	dst.PrependReactor("get", "tenants", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetResource().Version == "v1beta2" {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "capsule.clastix.io", Resource: "tenants"}, "acme", fmt.Errorf("no"))
		}
		return false, nil, nil
	})
	if err := Sync(context.Background(), dst, []Item{capsuleNamespace("app", "acme")}); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Resource(nsGVR()).Get(context.Background(), "app", metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
}

func nsGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}
}

func tenantGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "capsule.clastix.io", Version: "v1beta2", Resource: "tenants"}
}

func capsuleTenant(name string) Item {
	return Item{
		GVR: tenantGVR(),
		Obj: &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "capsule.clastix.io/v1beta2", "kind": "Tenant",
			"metadata": map[string]any{"name": name},
		}},
	}
}

func capsuleNamespace(name, tenant string) Item {
	return Item{
		GVR: nsGVR(),
		Obj: &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Namespace",
			"metadata": map[string]any{
				"name":   name,
				"labels": map[string]any{TenantLabel: tenant},
			},
		}},
	}
}
