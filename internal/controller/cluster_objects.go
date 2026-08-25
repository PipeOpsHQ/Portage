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

	"k8s.io/client-go/dynamic"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/clusterobjects"
	"github.com/PipeOpsHQ/portage/pkg/clusters"
	"github.com/PipeOpsHQ/portage/pkg/objectstore"
	"github.com/PipeOpsHQ/portage/pkg/transform"
)

// withClusterObjects appends the synthetic ObjectGraph workload when the
// Policy opted into live object-graph backup/replicate/restore.
func withClusterObjects(pol *portagev1alpha1.Policy, inv classify.Inventory) classify.Inventory {
	if pol == nil || !pol.Spec.ClusterObjects.Enabled {
		return inv
	}
	for _, w := range inv.Workloads {
		if clusterobjects.Is(w) {
			return inv
		}
	}
	ns := pol.Namespace
	if ns == "" && len(pol.Spec.Selector.Namespaces) > 0 {
		ns = pol.Spec.Selector.Namespaces[0]
	}
	out := make([]classify.Workload, len(inv.Workloads), len(inv.Workloads)+1)
	copy(out, inv.Workloads)
	out = append(out, clusterobjects.Synthetic(ns))
	inv.Workloads = out
	return inv
}

func (r *ActionReconciler) transformOpt(pair *portagev1alpha1.ClusterPair) transform.Options {
	opt := transform.Options{}
	if pair != nil {
		opt.StorageClassMap = pair.Spec.StorageClassMap
	}
	return opt
}

func dynOf(ep clusters.Endpoints, fallback dynamic.Interface) dynamic.Interface {
	if ep.Dynamic != nil {
		return ep.Dynamic
	}
	return fallback
}

func (r *ActionReconciler) listClusterObjects(ctx context.Context, pol *portagev1alpha1.Policy, src clusters.Endpoints) ([]clusterobjects.Item, error) {
	kube := src.Kube
	if kube == nil {
		kube = r.Kube
	}
	dyn := dynOf(src, r.Dynamic)
	if kube == nil || dyn == nil {
		return nil, fmt.Errorf("clusterobjects: kube and dynamic clients required")
	}
	nss := classify.Namespaces(pol.Spec.Selector.Namespaces, pol.Namespace)
	return clusterobjects.Capture(ctx, kube.Discovery(), dyn, pol.Spec.ClusterObjects, nss, transform.Options{})
}

func (r *ActionReconciler) captureClusterObjects(ctx context.Context, pol *portagev1alpha1.Policy, pair *portagev1alpha1.ClusterPair, ep clusters.Pair, w classify.Workload) (id string, size int64, useful bool, msg string, err error) {
	items, err := r.listClusterObjects(ctx, pol, ep.Source)
	if err != nil {
		return "", 0, false, err.Error(), err
	}
	items = clusterobjects.Sanitize(items, r.transformOpt(pair))
	raw, err := clusterobjects.Marshal(items)
	if err != nil {
		return "", 0, false, err.Error(), err
	}
	id = objectstore.Key(w.Namespace, w.Key(), "objects-"+r.now().UTC().Format("20060102T150405Z"))
	if err := r.store().Put(ctx, id, raw); err != nil {
		return "", 0, false, err.Error(), err
	}
	return id, int64(len(raw)), true, fmt.Sprintf("%d objects snapshot", len(items)), nil
}

func (r *ActionReconciler) syncClusterObjectsLive(ctx context.Context, pol *portagev1alpha1.Policy, pair *portagev1alpha1.ClusterPair, ep clusters.Pair) (bool, string, error) {
	items, err := r.listClusterObjects(ctx, pol, ep.Source)
	if err != nil {
		return false, err.Error(), err
	}
	items = clusterobjects.Sanitize(items, r.transformOpt(pair))
	dest := dynOf(ep.Dest, r.Dynamic)
	if dest == nil {
		return false, "dest dynamic client required", fmt.Errorf("clusterobjects: dest dynamic client required")
	}
	if err := clusterobjects.Sync(ctx, dest, items); err != nil {
		return false, err.Error(), err
	}
	ok, msg := clusterobjects.Attest(ctx, dest, items)
	return ok, msg, nil
}

func (r *ActionReconciler) restoreClusterObjects(ctx context.Context, pair *portagev1alpha1.ClusterPair, ep clusters.Pair, artifactID string) (bool, string, error) {
	raw, err := r.store().Get(ctx, artifactID)
	if err != nil {
		return false, err.Error(), err
	}
	items, err := clusterobjects.Unmarshal(raw)
	if err != nil {
		return false, err.Error(), err
	}
	items = clusterobjects.Sanitize(items, r.transformOpt(pair))
	dest := dynOf(ep.Dest, r.Dynamic)
	if dest == nil {
		return false, "dest dynamic client required", fmt.Errorf("clusterobjects: dest dynamic client required")
	}
	if err := clusterobjects.Sync(ctx, dest, items); err != nil {
		return false, err.Error(), err
	}
	ok, msg := clusterobjects.Attest(ctx, dest, items)
	return ok, msg, nil
}
