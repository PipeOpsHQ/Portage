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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// defaultCapsuleUserGroup is the group CapsuleConfiguration ships with.
// An Entra service-principal token does not carry it, so the subject is
// not a Capsule user until spec.users names them.
const defaultCapsuleUserGroup = "projectcapsule.dev"

// capsuleConfigGVRs are the CapsuleConfiguration versions a destination may serve.
var capsuleConfigGVRs = []schema.GroupVersionResource{
	{Group: capsuleGroup, Version: "v1beta2", Resource: "capsuleconfigurations"},
	{Group: capsuleGroup, Version: "v1beta1", Resource: "capsuleconfigurations"},
}

// capsuleAccess is what the destination records for this caller.
// read is false when the lookup did not run or could not see the objects.
// The webhook classifies a Capsule user from CapsuleConfiguration status.users,
// which is copied from spec.users and from TenantOwner objects whose
// aggregate flag is left at its default. Tenant status.owners is a separate
// list, copied from Tenant spec.owners. Both have to match.
type capsuleAccess struct {
	read          bool
	configRead    bool
	configName    string
	userInStatus  bool
	userInSpec    bool
	ownerInStatus bool
	ownerInSpec   bool
}

func observeCapsuleCaller(ctx context.Context, dyn dynamic.Interface, tenant string, caller Caller) capsuleAccess {
	if dyn == nil || caller.Name == "" || tenant == "" {
		return capsuleAccess{}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var access capsuleAccess
	if configs, ok := listCapsuleConfigurations(ctx, dyn); ok {
		access.read = true
		access.configRead = true
		if pick := pickCapsuleConfiguration(configs); pick != nil {
			access.configName = pick.GetName()
			statusUsers, _, _ := unstructured.NestedSlice(pick.Object, "status", "users")
			specUsers, _, _ := unstructured.NestedSlice(pick.Object, "spec", "users")
			access.userInStatus = subjectListed(statusUsers, caller)
			access.userInSpec = subjectListed(specUsers, caller) || legacyCapsuleUsers(pick, caller)
		}
	}
	tnt, err := getDestTenant(ctx, dyn, tenant)
	if err == nil && tnt != nil {
		access.read = true
		specOwners, _, _ := unstructured.NestedSlice(tnt.Object, "spec", "owners")
		statusOwners, _, _ := unstructured.NestedSlice(tnt.Object, "status", "owners")
		access.ownerInSpec = subjectListed(specOwners, caller)
		access.ownerInStatus = subjectListed(statusOwners, caller)
	}
	return access
}

func listCapsuleConfigurations(ctx context.Context, dyn dynamic.Interface) (items []unstructured.Unstructured, ok bool) {
	// The dynamic fake panics when a list kind was not registered. A real
	// API server returns an error for an unknown resource. Either way a
	// failed list must not crash the reconcile.
	defer func() {
		if recover() != nil {
			items, ok = nil, false
		}
	}()
	sawSuccess := false
	for _, gvr := range capsuleConfigGVRs {
		list, err := dyn.Resource(gvr).List(ctx, metav1.ListOptions{})
		if err == nil {
			sawSuccess = true
			if list != nil && len(list.Items) > 0 {
				return list.Items, true
			}
			continue
		}
		if apierrors.IsNotFound(err) || isNoMatch(err) {
			continue
		}
		return nil, false
	}
	if sawSuccess {
		return nil, true
	}
	return nil, false
}

func pickCapsuleConfiguration(items []unstructured.Unstructured) *unstructured.Unstructured {
	if len(items) == 0 {
		return nil
	}
	for i := range items {
		if items[i].GetName() == "default" {
			return &items[i]
		}
	}
	return &items[0]
}

func getDestTenant(ctx context.Context, dyn dynamic.Interface, name string) (*unstructured.Unstructured, error) {
	var denied error
	for _, gvr := range tenantGVRs {
		obj, err := dyn.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			return obj, nil
		}
		if apierrors.IsNotFound(err) || isNoMatch(err) {
			continue
		}
		denied = err
	}
	return nil, denied
}

func subjectListed(items []any, caller Caller) bool {
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		kind, _, _ := unstructured.NestedString(m, "kind")
		name, _, _ := unstructured.NestedString(m, "name")
		if name == "" {
			continue
		}
		switch kind {
		case "User", "ServiceAccount":
			if name == caller.Name {
				return true
			}
		case "Group":
			if groupContains(caller.Groups, name) {
				return true
			}
		}
	}
	return false
}

func legacyCapsuleUsers(obj *unstructured.Unstructured, caller Caller) bool {
	names, _, _ := unstructured.NestedStringSlice(obj.Object, "spec", "userNames")
	if groupContains(names, caller.Name) {
		return true
	}
	groups, _, _ := unstructured.NestedStringSlice(obj.Object, "spec", "userGroups")
	for _, group := range groups {
		if groupContains(caller.Groups, group) {
			return true
		}
	}
	return false
}

func groupContains(groups []string, want string) bool {
	for _, group := range groups {
		if group == want {
			return true
		}
	}
	return false
}

func capsuleGap(tenant string, caller Caller, access capsuleAccess) string {
	register := fmt.Sprintf(
		"Register it on Tenant %q spec.owners (kind: %s, name: %q) and on CapsuleConfiguration spec.users with the same kind and name. Capsule's namespace mutating webhook stamps a Tenant ownerReference only when the caller is a Capsule user or a Capsule administrator. Tenant spec.owners does not grant that, so the validating webhook then rejects the tenant label with no ownerReference. The default Capsule user group %s is not added to an Entra service principal token. A TenantOwner labeled %s=%s with this subject registers both, because spec.aggregate defaults to true",
		tenant, caller.Kind, caller.Name, defaultCapsuleUserGroup, "projectcapsule.dev/tenant", tenant,
	)
	if !access.read {
		return register
	}
	switch {
	case access.userInStatus && access.ownerInStatus:
		return fmt.Sprintf(
			"Tenant %q status.owners and CapsuleConfiguration status.users both already list %s %q. The namespace mutating webhook still did not stamp a Tenant ownerReference before validation rejected the tenant label. Confirm that webhook is registered for namespace CREATE and its failurePolicy is Fail",
			tenant, caller.Kind, caller.Name,
		)
	case access.userInSpec && !access.userInStatus && access.ownerInStatus:
		return fmt.Sprintf(
			"Tenant %q status.owners lists %s %q. CapsuleConfiguration spec.users lists that subject and status.users does not yet. The mutating webhook reads status.users. Wait until the configuration controller copies spec.users into status.users",
			tenant, caller.Kind, caller.Name,
		)
	case access.ownerInStatus && !access.userInStatus && !access.configRead:
		return fmt.Sprintf(
			"Tenant %q status.owners already lists %s %q. This client could not read CapsuleConfiguration. The namespace mutating webhook skips a caller who is absent from its status.users, and the validating webhook then rejects the tenant label with no ownerReference. Add that subject to CapsuleConfiguration spec.users (kind: %s, name: %q) when status.users does not already list it.%s",
			tenant, caller.Kind, caller.Name, caller.Kind, caller.Name, groupNote(caller),
		)
	case access.ownerInStatus && !access.userInStatus:
		where := "No CapsuleConfiguration on the destination lists that subject in status.users"
		target := "CapsuleConfiguration spec.users"
		if access.configName != "" {
			where = fmt.Sprintf("CapsuleConfiguration %q status.users does not", access.configName)
			target = fmt.Sprintf("CapsuleConfiguration %q spec.users", access.configName)
		}
		return fmt.Sprintf(
			"Tenant %q status.owners already lists %s %q. %s, so the namespace mutating webhook skips this caller and does not write the ownerReference. Add that subject to %s (kind: %s, name: %q). The webhook reads status.users, which the configuration controller copies from spec.users. The validating webhook then rejects the tenant label with no ownerReference.%s",
			tenant, caller.Kind, caller.Name, where, target, caller.Kind, caller.Name, groupNote(caller),
		)
	case access.userInStatus && !access.ownerInStatus:
		return fmt.Sprintf(
			"CapsuleConfiguration status.users already lists %s %q. Tenant %q spec.owners and status.owners do not. Add that subject to Tenant %q spec.owners (kind: %s, name: %q). The mutating webhook assigns the labeled tenant only when status.owners matches the caller",
			caller.Kind, caller.Name, tenant, tenant, caller.Kind, caller.Name,
		)
	case access.ownerInSpec && !access.ownerInStatus:
		return fmt.Sprintf(
			"Tenant %q spec.owners lists %s %q and status.owners does not yet. The Tenant controller copies spec.owners into status.owners before admission treats the subject as an owner. Also add the subject to CapsuleConfiguration spec.users (kind: %s, name: %q) when status.users does not already list it. The mutating webhook reads that status, not Tenant spec.owners",
			tenant, caller.Kind, caller.Name, caller.Kind, caller.Name,
		)
	default:
		return register
	}
}

func groupNote(caller Caller) string {
	if groupContains(caller.Groups, defaultCapsuleUserGroup) {
		return ""
	}
	if len(caller.Groups) == 0 {
		return " SelfSubjectReview returned no groups. The default Capsule user group " + defaultCapsuleUserGroup + " is not on this token."
	}
	shown := caller.Groups
	extra := ""
	if len(shown) > 6 {
		extra = fmt.Sprintf(" and %d more", len(shown)-6)
		shown = shown[:6]
	}
	return fmt.Sprintf(
		" SelfSubjectReview groups are %s%s. The default Capsule user group %s is not among them.",
		strings.Join(shown, ", "), extra, defaultCapsuleUserGroup,
	)
}
