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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
)

func (r Resolver) restConfig(ctx context.Context, ref portagev1alpha1.ClusterRef) (*rest.Config, error) {
	if n := ref.AuthMethods(); n > 1 {
		return nil, fmt.Errorf("cluster %s: set only one of kubeconfigSecret, azure, aws, gcp", ref.Name)
	}
	switch {
	case ref.KubeconfigSecret != nil:
		return r.kubeconfigREST(ctx, ref)
	case ref.Azure != nil, ref.AWS != nil, ref.GCP != nil:
		if r.Cloud != nil {
			return r.Cloud(ctx, ref)
		}
		return r.cloudREST(ctx, ref)
	default:
		return nil, fmt.Errorf("cluster %s: no auth method", ref.Name)
	}
}

func (r Resolver) kubeconfigREST(ctx context.Context, ref portagev1alpha1.ClusterRef) (*rest.Config, error) {
	ns := ref.KubeconfigSecret.Namespace
	if ns == "" {
		ns = r.HubNS
	}
	if ns == "" {
		ns = "portage-system"
	}
	keyName := ref.KubeconfigSecret.Key
	if keyName == "" {
		keyName = "kubeconfig"
	}
	raw, err := r.secretBytes(ctx, ns, ref.KubeconfigSecret.Name, keyName)
	if err != nil {
		return nil, err
	}
	return clientcmd.RESTConfigFromKubeConfig(raw)
}

func (r Resolver) cloudREST(ctx context.Context, ref portagev1alpha1.ClusterRef) (*rest.Config, error) {
	switch {
	case ref.Azure != nil:
		return r.azureREST(ctx, ref)
	case ref.AWS != nil:
		return r.awsREST(ctx, ref)
	case ref.GCP != nil:
		return r.gcpREST(ctx, ref)
	default:
		return nil, fmt.Errorf("cluster %s: no cloud auth", ref.Name)
	}
}

func (r Resolver) credSecret(ctx context.Context, ref *portagev1alpha1.SecretKeyRef) (map[string][]byte, error) {
	if ref == nil {
		return nil, nil
	}
	ns := ref.Namespace
	if ns == "" {
		ns = r.HubNS
	}
	if ns == "" {
		ns = "portage-system"
	}
	if r.Secrets != nil {
		sec, err := r.Secrets.CoreV1().Secrets(ns).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		return sec.Data, nil
	}
	if r.Hub == nil {
		return nil, fmt.Errorf("hub client required to load credentials secret")
	}
	sec := &corev1.Secret{}
	if err := r.Hub.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: ns}, sec); err != nil {
		return nil, err
	}
	return sec.Data, nil
}

func secretString(data map[string][]byte, keys ...string) string {
	for _, k := range keys {
		if v, ok := data[k]; ok && len(v) > 0 {
			return string(v)
		}
	}
	return ""
}
