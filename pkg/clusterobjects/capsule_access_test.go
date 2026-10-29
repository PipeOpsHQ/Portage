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
	"testing"
)

func TestCapsuleGapNamesMissingCapsuleUser(t *testing.T) {
	t.Parallel()
	const oid = "396e2032-ba4e-4ebf-ae71-1bbc85666502"
	caller := Caller{Kind: "User", Name: oid, Groups: []string{"system:authenticated"}}
	access := capsuleAccess{
		read:          true,
		configRead:    true,
		configName:    "default",
		ownerInStatus: true,
	}
	msg := ownerHint("elegant-network", caller, access)
	for _, want := range []string{
		"status.owners already lists User",
		oid,
		`CapsuleConfiguration "default" status.users does not`,
		`spec.users (kind: User, name: "` + oid + `")`,
		"projectcapsule.dev",
		"system:authenticated",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("missing %q in %s", want, msg)
		}
	}
	if strings.Contains(msg, "portage-controller") {
		t.Fatalf("hub ServiceAccount named: %s", msg)
	}
}

func TestCapsuleGapWhenBothRegistrationsExist(t *testing.T) {
	t.Parallel()
	caller := Caller{Kind: "User", Name: "oid", Groups: []string{"system:authenticated"}}
	msg := capsuleGap("elegant-network", caller, capsuleAccess{
		read:          true,
		configName:    "default",
		userInStatus:  true,
		ownerInStatus: true,
	})
	if !strings.Contains(msg, "both already list") || !strings.Contains(msg, "failurePolicy is Fail") {
		t.Fatalf("got %s", msg)
	}
}

func TestCapsuleGapWithoutAReadStillNamesBothObjects(t *testing.T) {
	t.Parallel()
	msg := ownerHint("elegant-network", Caller{}, capsuleAccess{})
	for _, want := range []string{"spec.owners", "spec.users", "clusterUser", "clientID", "projectcapsule.dev"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("missing %q in %s", want, msg)
		}
	}
}

func TestSubjectListedMatchesUserAndGroup(t *testing.T) {
	t.Parallel()
	owners := []any{
		map[string]any{"kind": "User", "name": "396e2032-ba4e-4ebf-ae71-1bbc85666502"},
		map[string]any{"kind": "Group", "name": "projectcapsule.dev"},
	}
	user := Caller{Kind: "User", Name: "396e2032-ba4e-4ebf-ae71-1bbc85666502"}
	if !subjectListed(owners, user) {
		t.Fatal("user owner did not match")
	}
	grouped := Caller{Kind: "User", Name: "other", Groups: []string{"projectcapsule.dev"}}
	if !subjectListed(owners, grouped) {
		t.Fatal("group owner did not match token groups")
	}
	oidAsGroup := Caller{Kind: "User", Name: "other", Groups: []string{"system:authenticated"}}
	owners = []any{map[string]any{"kind": "Group", "name": "396e2032-ba4e-4ebf-ae71-1bbc85666502"}}
	if subjectListed(owners, oidAsGroup) {
		t.Fatal("group owner name must match a token group, not stand in for the username")
	}
}
