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
	"testing"

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
