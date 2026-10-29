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
	"strings"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

const serviceAccountUsernamePrefix = "system:serviceaccount:"

// Caller is the subject the destination API server authenticated for this
// client. Kind is ServiceAccount only when the username has the
// system:serviceaccount: prefix. Every other username, including an Entra
// object ID, is a User. Name is the full username. Groups are the groups
// on that same SelfSubjectReview. Capsule treats a Group owner as a match
// only when one of these groups equals the owner name.
type Caller struct {
	Kind   string
	Name   string
	Groups []string
}

func subjectFromUsername(username string) Caller {
	if username == "" {
		return Caller{}
	}
	if strings.HasPrefix(username, serviceAccountUsernamePrefix) {
		return Caller{Kind: "ServiceAccount", Name: username}
	}
	return Caller{Kind: "User", Name: username}
}

var selfSubjectReviewGVR = schema.GroupVersionResource{
	Group: "authentication.k8s.io", Version: "v1", Resource: "selfsubjectreviews",
}

// CallerFromDynamic asks the destination API server who this dynamic client
// is. An error or an empty username returns a zero Caller so the admission
// hint can fall back without naming a hub ServiceAccount.
func CallerFromDynamic(ctx context.Context, dyn dynamic.Interface) Caller {
	if dyn == nil {
		return Caller{}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "authentication.k8s.io/v1",
		"kind":       "SelfSubjectReview",
	}}
	created, err := dyn.Resource(selfSubjectReviewGVR).Create(ctx, obj, metav1.CreateOptions{})
	if err != nil || created == nil {
		return Caller{}
	}
	username, _, _ := unstructured.NestedString(created.Object, "status", "userInfo", "username")
	groups, _, _ := unstructured.NestedStringSlice(created.Object, "status", "userInfo", "groups")
	caller := subjectFromUsername(username)
	caller.Groups = groups
	return caller
}

// CallerFromKube is CallerFromDynamic for a typed clientset. A client that
// cannot create SelfSubjectReview (including a fake clientset) returns a
// zero Caller.
func CallerFromKube(ctx context.Context, kube kubernetes.Interface) (caller Caller) {
	if kube == nil {
		return Caller{}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	defer func() {
		if recover() != nil {
			caller = Caller{}
		}
	}()
	review, err := kube.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil || review == nil {
		return Caller{}
	}
	caller = subjectFromUsername(review.Status.UserInfo.Username)
	caller.Groups = review.Status.UserInfo.Groups
	return caller
}
