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

package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
)

// ClusterPairReconciler pings source and dest APIs.
type ClusterPairReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	KubeClient kubernetes.Interface
}

// +kubebuilder:rbac:groups=portage.io,resources=clusterpairs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=portage.io,resources=clusterpairs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *ClusterPairReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pair := &portagev1alpha1.ClusterPair{}
	if err := r.Get(ctx, req.NamespacedName, pair); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	srcOK := r.ping(ctx, r.KubeClient)
	dstOK, dstMsg := r.pingRef(ctx, pair.Spec.Destination)
	pair.Status.SourceReachable = srcOK
	pair.Status.DestinationReachable = dstOK
	pair.Status.ObservedGeneration = pair.Generation
	switch {
	case srcOK && dstOK:
		pair.Status.Phase = portagev1alpha1.ClusterPairReady
		pair.Status.Message = "source and destination reachable"
	case !srcOK:
		pair.Status.Phase = portagev1alpha1.ClusterPairFailed
		pair.Status.Message = "source cluster unreachable"
	default:
		pair.Status.Phase = portagev1alpha1.ClusterPairDegraded
		pair.Status.Message = dstMsg
	}
	if err := r.Status().Update(ctx, pair); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 2 * time.Minute}, nil
}

func (r *ClusterPairReconciler) ping(ctx context.Context, kube kubernetes.Interface) bool {
	if kube == nil {
		return false
	}
	_, err := kube.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	return err == nil
}

func (r *ClusterPairReconciler) pingRef(ctx context.Context, ref portagev1alpha1.ClusterRef) (bool, string) {
	if ref.KubeconfigSecret == nil {
		if r.ping(ctx, r.KubeClient) {
			return true, "destination is local cluster"
		}
		return false, "local destination unreachable"
	}
	ns := ref.KubeconfigSecret.Namespace
	if ns == "" {
		ns = "portage-system"
	}
	key := types.NamespacedName{Name: ref.KubeconfigSecret.Name, Namespace: ns}
	sec := &corev1.Secret{}
	if err := r.Get(ctx, key, sec); err != nil {
		return false, "kubeconfig secret: " + err.Error()
	}
	k := ref.KubeconfigSecret.Key
	if k == "" {
		k = "kubeconfig"
	}
	raw, ok := sec.Data[k]
	if !ok {
		return false, fmt.Sprintf("secret %s missing key %s", key.Name, k)
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig(raw)
	if err != nil {
		return false, "kubeconfig parse: " + err.Error()
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return false, err.Error()
	}
	if !r.ping(ctx, cs) {
		return false, "destination API ping failed"
	}
	return true, ""
}

func (r *ClusterPairReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&portagev1alpha1.ClusterPair{}).
		Complete(r)
}
