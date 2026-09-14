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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/clusters"
)

// ClusterPairReconciler pings source and dest APIs.
type ClusterPairReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	KubeClient kubernetes.Interface
	Resolve    func(context.Context, *portagev1alpha1.ClusterPair) (clusters.Pair, error)
}

// +kubebuilder:rbac:groups=portage.io,resources=clusterpairs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=portage.io,resources=clusterpairs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *ClusterPairReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pair := &portagev1alpha1.ClusterPair{}
	if err := r.Get(ctx, req.NamespacedName, pair); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	srcOK, srcMsg, dstOK, dstMsg := r.pingPair(ctx, pair)
	pair.Status.SourceReachable = srcOK
	pair.Status.DestinationReachable = dstOK
	pair.Status.ObservedGeneration = pair.Generation
	switch {
	case srcOK && dstOK:
		pair.Status.Phase = portagev1alpha1.ClusterPairReady
		pair.Status.Message = "source and destination reachable"
	case !srcOK:
		pair.Status.Phase = portagev1alpha1.ClusterPairFailed
		if srcMsg != "" {
			pair.Status.Message = "source cluster: " + srcMsg
		} else {
			pair.Status.Message = "source cluster unreachable"
		}
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

func (r *ClusterPairReconciler) pingPair(ctx context.Context, pair *portagev1alpha1.ClusterPair) (srcOK bool, srcMsg string, dstOK bool, dstMsg string) {
	srcRemote := pair.Spec.Source.HasRemoteAuth()
	dstRemote := pair.Spec.Destination.HasRemoteAuth()
	if !srcRemote && !dstRemote {
		ok := r.ping(ctx, r.KubeClient)
		msg := ""
		if !ok {
			msg = "local cluster unreachable"
		}
		return ok, msg, ok, msg
	}
	if r.Resolve == nil {
		return false, "cluster resolver is not configured", false, "cluster resolver is not configured"
	}
	ep, err := r.Resolve(ctx, pair)
	if err != nil {
		if srcRemote {
			return false, err.Error(), false, ""
		}
		return r.ping(ctx, r.KubeClient), "", false, err.Error()
	}
	srcOK = r.ping(ctx, ep.Source.Kube)
	dstOK = r.ping(ctx, ep.Dest.Kube)
	if !srcOK {
		srcMsg = "API ping failed"
	}
	if !dstOK {
		dstMsg = "destination API ping failed"
	}
	return srcOK, srcMsg, dstOK, dstMsg
}

func (r *ClusterPairReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&portagev1alpha1.ClusterPair{}).
		Complete(r)
}
