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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestEnsureCapsuleCallerAppendsOwnerOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dst := dynfake.NewSimpleDynamicClient(runtime.NewScheme())
	if _, err := dst.Resource(tenantGVR()).Create(ctx, capsuleTenant("acme").Obj, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	caller := Caller{Kind: "User", Name: "396e2032-ba4e-4ebf-ae71-1bbc85666502"}
	if err := ensureCapsuleCaller(ctx, dst, "acme", caller); err != nil {
		t.Fatal(err)
	}
	if err := ensureCapsuleCaller(ctx, dst, "acme", caller); err != nil {
		t.Fatal(err)
	}
	if err := ensureCapsuleCaller(ctx, dst, "acme", Caller{}); err != nil {
		t.Fatal(err)
	}
	got, err := dst.Resource(tenantGVR()).Get(ctx, "acme", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	owners, _, _ := unstructured.NestedSlice(got.Object, "spec", "owners")
	n := 0
	for _, owner := range owners {
		if key, ok := ownerIdentity(owner); ok && key == "User\x00396e2032-ba4e-4ebf-ae71-1bbc85666502" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("owners %#v", owners)
	}
}

func TestEnsureTenantOwnerKeepsForeignStableName(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dst := dynfake.NewSimpleDynamicClient(runtime.NewScheme())
	if _, err := dst.Resource(tenantGVR()).Create(ctx, capsuleTenant("acme").Obj, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	foreign := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "capsule.clastix.io/v1beta2",
		"kind":       "TenantOwner",
		"metadata":   map[string]any{"name": "portage-acme"},
		"spec":       map[string]any{"kind": "User", "name": "someone-else"},
	}}
	if _, err := dst.Resource(ownerGVR()).Create(ctx, foreign, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	caller := Caller{Kind: "User", Name: "396e2032-ba4e-4ebf-ae71-1bbc85666502"}
	if err := ensureCapsuleCaller(ctx, dst, "acme", caller); err != nil {
		t.Fatal(err)
	}
	kept, err := dst.Resource(ownerGVR()).Get(ctx, "portage-acme", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !tenantOwnerSubject(kept, Caller{Kind: "User", Name: "someone-else"}) {
		t.Fatalf("stable name was overwritten: %#v", kept.Object["spec"])
	}
	alt, err := dst.Resource(ownerGVR()).Get(ctx, tenantOwnerName("acme", callerSuffix(caller)), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !tenantOwnerSubject(alt, caller) || alt.GetLabels()[tenantOwnerLabel] != "acme" {
		t.Fatalf("alternate TenantOwner %#v labels %#v", alt.Object["spec"], alt.GetLabels())
	}
}

func TestEnsureCapsuleCallerSurfacesForbidden(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dst := dynfake.NewSimpleDynamicClient(runtime.NewScheme())
	if _, err := dst.Resource(tenantGVR()).Create(ctx, capsuleTenant("acme").Obj, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	dst.PrependReactor("update", "tenants", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: capsuleGroup, Resource: "tenants"}, "acme", fmt.Errorf("rbac"))
	})
	err := ensureCapsuleCaller(ctx, dst, "acme", Caller{Kind: "User", Name: "oid"})
	if err == nil || !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), "spec.owners") {
		t.Fatalf("got %v", err)
	}
}

func TestSyncCreatesNamespaceOnceStatusOwnersListsCaller(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const oid = "11111111-2222-3333-4444-555555555555"
	cfgGVR := schema.GroupVersionResource{Group: capsuleGroup, Version: "v1beta2", Resource: "capsuleconfigurations"}
	dst := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		cfgGVR: "CapsuleConfigurationList",
	})
	owner := map[string]any{"kind": "User", "name": oid, "clusterRoles": []any{"admin"}}
	tenant := capsuleTenant("acme")
	_ = unstructured.SetNestedSlice(tenant.Obj.Object, []any{owner}, "spec", "owners")
	_ = unstructured.SetNestedSlice(tenant.Obj.Object, []any{owner}, "status", "owners")
	if _, err := dst.Resource(tenantGVR()).Create(ctx, tenant.Obj, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	cfg := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "capsule.clastix.io/v1beta2",
		"kind":       "CapsuleConfiguration",
		"metadata":   map[string]any{"name": "default"},
		"status": map[string]any{
			"users": []any{map[string]any{"kind": "User", "name": oid}},
		},
	}}
	if _, err := dst.Resource(cfgGVR).Create(ctx, cfg, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	dst.PrependReactor("create", "selfsubjectreviews", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "authentication.k8s.io/v1",
			"kind":       "SelfSubjectReview",
			"status":     map[string]any{"userInfo": map[string]any{"username": oid}},
		}}, nil
	})
	if err := Sync(ctx, dst, []Item{capsuleNamespace("app", "acme")}); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Resource(nsGVR()).Get(ctx, "app", metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Resource(ownerGVR()).Get(ctx, "portage-acme", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("status.users already lists the caller, TenantOwner get %v", err)
	}
}
