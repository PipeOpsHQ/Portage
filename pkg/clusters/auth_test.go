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

package clusters

import (
	"testing"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
)

func TestClusterRefAuthMethods(t *testing.T) {
	t.Parallel()
	if (portagev1alpha1.ClusterRef{Name: "local"}).HasRemoteAuth() {
		t.Fatal("empty auth is in-cluster")
	}
	refs := []portagev1alpha1.ClusterRef{
		{KubeconfigSecret: &portagev1alpha1.SecretKeyRef{Name: "k"}},
		{Azure: &portagev1alpha1.AzureAuth{ResourceID: "/x"}},
		{AWS: &portagev1alpha1.AWSAuth{ClusterName: "c", Region: "r"}},
		{GCP: &portagev1alpha1.GCPAuth{Project: "p", Location: "l", Cluster: "c"}},
	}
	for i, r := range refs {
		if r.AuthMethods() != 1 || !r.HasRemoteAuth() {
			t.Fatalf("%d methods=%d", i, r.AuthMethods())
		}
	}
	both := portagev1alpha1.ClusterRef{
		KubeconfigSecret: &portagev1alpha1.SecretKeyRef{Name: "k"},
		AWS:              &portagev1alpha1.AWSAuth{ClusterName: "c", Region: "r"},
	}
	if both.AuthMethods() != 2 {
		t.Fatalf("got %d", both.AuthMethods())
	}
}
