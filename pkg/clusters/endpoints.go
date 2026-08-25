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

// Package clusters resolves source and destination API clients from a ClusterPair.
package clusters

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/kubeexec"
)

// Endpoints is one cluster the hub can talk to.
type Endpoints struct {
	Name    string
	Kube    kubernetes.Interface
	Dynamic dynamic.Interface
	Exec    kubeexec.Interface
	REST    *rest.Config
}

// Pair is source + dest. When dest kubeconfig is empty, Dest == Source (in-cluster).
type Pair struct {
	Source Endpoints
	Dest   Endpoints
}

// Local is the hub cluster.
func Local(name string, kube kubernetes.Interface, dyn dynamic.Interface, exec kubeexec.Interface, cfg *rest.Config) Endpoints {
	return Endpoints{Name: name, Kube: kube, Dynamic: dyn, Exec: exec, REST: cfg}
}

// Resolver builds a Pair from a ClusterPair CR.
type Resolver struct {
	Hub       client.Client
	Local     Endpoints
	HubNS     string
	NewForCfg func(*rest.Config) (kubernetes.Interface, dynamic.Interface, kubeexec.Interface, error)
}

// Resolve returns source and dest endpoints. Empty kubeconfig secret ⇒ local.
func (r Resolver) Resolve(ctx context.Context, pair *portagev1alpha1.ClusterPair) (Pair, error) {
	src := r.Local
	dst := r.Local
	if pair == nil {
		return Pair{Source: src, Dest: dst}, nil
	}
	src.Name = pair.Spec.Source.Name
	dst.Name = pair.Spec.Destination.Name
	var err error
	if pair.Spec.Source.KubeconfigSecret != nil {
		src, err = r.fromSecret(ctx, pair.Spec.Source)
		if err != nil {
			return Pair{}, fmt.Errorf("source cluster: %w", err)
		}
	}
	if pair.Spec.Destination.KubeconfigSecret != nil {
		dst, err = r.fromSecret(ctx, pair.Spec.Destination)
		if err != nil {
			return Pair{}, fmt.Errorf("destination cluster: %w", err)
		}
	}
	return Pair{Source: src, Dest: dst}, nil
}

func (r Resolver) fromSecret(ctx context.Context, ref portagev1alpha1.ClusterRef) (Endpoints, error) {
	if r.Hub == nil {
		return Endpoints{}, fmt.Errorf("hub client required to load kubeconfig secret")
	}
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
	sec := &corev1.Secret{}
	if err := r.Hub.Get(ctx, types.NamespacedName{Name: ref.KubeconfigSecret.Name, Namespace: ns}, sec); err != nil {
		return Endpoints{}, err
	}
	raw, ok := sec.Data[keyName]
	if !ok {
		return Endpoints{}, fmt.Errorf("secret %s/%s missing key %s", ns, ref.KubeconfigSecret.Name, keyName)
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig(raw)
	if err != nil {
		return Endpoints{}, err
	}
	newFn := r.NewForCfg
	if newFn == nil {
		newFn = defaultNew
	}
	kube, dyn, exec, err := newFn(cfg)
	if err != nil {
		return Endpoints{}, err
	}
	return Endpoints{Name: ref.Name, Kube: kube, Dynamic: dyn, Exec: exec, REST: cfg}, nil
}

func defaultNew(cfg *rest.Config) (kubernetes.Interface, dynamic.Interface, kubeexec.Interface, error) {
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	return kube, dyn, kubeexec.SPDY{Config: cfg, Client: kube}, nil
}
