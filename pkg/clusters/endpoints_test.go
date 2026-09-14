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
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/kubeexec"
)

const miniKubeconfig = `apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://dest.example:6443
    insecure-skip-tls-verify: true
  name: dest
contexts:
- context:
    cluster: dest
    user: dest
  name: dest
current-context: dest
users:
- name: dest
  user:
    token: e2e
`

func TestResolveLoadsDestKubeconfigViaSecretsClient(t *testing.T) {
	t.Parallel()
	hub := k8sfake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "dest-kube", Namespace: "portage-system"},
		Data:       map[string][]byte{"kubeconfig": []byte(miniKubeconfig)},
	})
	destKube := k8sfake.NewSimpleClientset()
	r := Resolver{
		Secrets: hub,
		Local:   Local("local", hub, nil, nil, nil),
		HubNS:   "portage-system",
		NewForCfg: func(*rest.Config) (kubernetes.Interface, dynamic.Interface, kubeexec.Interface, error) {
			return destKube, nil, nil, nil
		},
	}
	pair := &portagev1alpha1.ClusterPair{
		Spec: portagev1alpha1.ClusterPairSpec{
			Source: portagev1alpha1.ClusterRef{Name: "src"},
			Destination: portagev1alpha1.ClusterRef{
				Name: "dst",
				KubeconfigSecret: &portagev1alpha1.SecretKeyRef{
					Name: "dest-kube", Namespace: "portage-system",
				},
			},
		},
	}
	p, err := r.Resolve(context.Background(), pair)
	if err != nil {
		t.Fatal(err)
	}
	if p.Dest.Kube != destKube {
		t.Fatal("dest client must come from dest kubeconfig, not hub")
	}
	if p.Source.Kube != hub {
		t.Fatal("empty source secret stays local")
	}
}

func TestResolveRejectsMultipleAuth(t *testing.T) {
	t.Parallel()
	r := Resolver{Local: Local("local", k8sfake.NewSimpleClientset(), nil, nil, nil)}
	pair := &portagev1alpha1.ClusterPair{
		Spec: portagev1alpha1.ClusterPairSpec{
			Source: portagev1alpha1.ClusterRef{Name: "src"},
			Destination: portagev1alpha1.ClusterRef{
				Name:             "dst",
				KubeconfigSecret: &portagev1alpha1.SecretKeyRef{Name: "k"},
				AWS:              &portagev1alpha1.AWSAuth{ClusterName: "prod", Region: "us-east-1"},
			},
		},
	}
	if _, err := r.Resolve(context.Background(), pair); err == nil {
		t.Fatal("expected error for kubeconfigSecret + aws")
	}
}

func TestResolveCloudAuth(t *testing.T) {
	t.Parallel()
	destKube := k8sfake.NewSimpleClientset()
	hub := k8sfake.NewSimpleClientset()
	tests := []struct {
		name string
		ref  portagev1alpha1.ClusterRef
	}{
		{name: "azure", ref: portagev1alpha1.ClusterRef{Name: "aks", Azure: &portagev1alpha1.AzureAuth{ResourceID: "/subscriptions/s/resourceGroups/rg/providers/Microsoft.ContainerService/managedClusters/c"}}},
		{name: "aws", ref: portagev1alpha1.ClusterRef{Name: "eks", AWS: &portagev1alpha1.AWSAuth{ClusterName: "prod", Region: "us-east-1"}}},
		{name: "gcp", ref: portagev1alpha1.ClusterRef{Name: "gke", GCP: &portagev1alpha1.GCPAuth{Project: "p", Location: "us-central1", Cluster: "c"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := Resolver{
				Local: Local("local", hub, nil, nil, nil),
				Cloud: func(_ context.Context, ref portagev1alpha1.ClusterRef) (*rest.Config, error) {
					if ref.Name != tt.ref.Name {
						t.Fatalf("ref name=%s", ref.Name)
					}
					return &rest.Config{Host: "https://" + tt.name + ".example"}, nil
				},
				NewForCfg: func(cfg *rest.Config) (kubernetes.Interface, dynamic.Interface, kubeexec.Interface, error) {
					if cfg.Host != "https://"+tt.name+".example" {
						t.Fatalf("host=%s", cfg.Host)
					}
					return destKube, nil, nil, nil
				},
			}
			pair := &portagev1alpha1.ClusterPair{
				Spec: portagev1alpha1.ClusterPairSpec{
					Source:      portagev1alpha1.ClusterRef{Name: "src"},
					Destination: tt.ref,
				},
			}
			p, err := r.Resolve(context.Background(), pair)
			if err != nil {
				t.Fatal(err)
			}
			if p.Dest.Kube != destKube {
				t.Fatal("dest client must come from cloud auth")
			}
			if p.Source.Kube != hub {
				t.Fatal("empty source stays local")
			}
		})
	}
}

func TestParseAKSResourceID(t *testing.T) {
	t.Parallel()
	got, err := parseAKSResourceID("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.ContainerService/managedClusters/aks")
	if err != nil {
		t.Fatal(err)
	}
	if got.Subscription != "sub" || got.ResourceGroup != "rg" || got.Cluster != "aks" {
		t.Fatalf("%+v", got)
	}
	if _, err := parseAKSResourceID("/not/aks"); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestEKSTokenFromURL(t *testing.T) {
	t.Parallel()
	tok := eksTokenFromURL("https://sts.amazonaws.com/?Action=GetCallerIdentity")
	if tok[:len(eksTokenPrefix)] != eksTokenPrefix {
		t.Fatalf("prefix %s", tok)
	}
}

func TestGKEResourceName(t *testing.T) {
	t.Parallel()
	got := gkeResourceName("p", "us-central1", "c")
	if got != "projects/p/locations/us-central1/clusters/c" {
		t.Fatalf("%s", got)
	}
}

func TestRESTConfigRequiresCA(t *testing.T) {
	t.Parallel()
	_, err := restConfig("https://api.example", nil, &cachedToken{fetch: func(context.Context) (string, time.Time, error) {
		return "t", time.Now().Add(time.Hour), nil
	}})
	if err == nil {
		t.Fatal("empty CA must fail")
	}
}

func TestCachedTokenRefresh(t *testing.T) {
	t.Parallel()
	n := 0
	c := &cachedToken{
		skew: time.Minute,
		fetch: func(context.Context) (string, time.Time, error) {
			n++
			return fmt.Sprintf("t%d", n), time.Now().Add(time.Hour), nil
		},
	}
	a, err := c.Token(context.Background())
	if err != nil || a != "t1" {
		t.Fatalf("%s %v", a, err)
	}
	b, err := c.Token(context.Background())
	if err != nil || b != "t1" || n != 1 {
		t.Fatalf("cache miss n=%d tok=%s", n, b)
	}
}
