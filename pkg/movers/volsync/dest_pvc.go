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

package volsync

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/PipeOpsHQ/portage/pkg/clusterobjects"
	"github.com/PipeOpsHQ/portage/pkg/transform"
)

// ensureNamespace creates the destination namespace when it is missing.
// labels carries the source Capsule tenant label when the source namespace
// has one. Creating the namespace bare and patching the label on later is
// rejected by Capsule 0.14, and that namespace can never be adopted.
// An existing namespace is left as-is so PVC replication into a namespace
// Portage already created can continue; cluster-object sync reports that
// Capsule will not adopt it.
func ensureNamespace(ctx context.Context, kube kubernetes.Interface, ns string, labels map[string]string) error {
	if kube == nil || ns == "" {
		return nil
	}
	_, err := kube.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}
	obj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, Labels: labels}}
	_, err = kube.CoreV1().Namespaces().Create(ctx, obj, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		return nil
	}
	if err != nil {
		tenant := ""
		if labels != nil {
			tenant = labels[clusterobjects.TenantLabel]
		}
		var caller clusterobjects.Caller
		if tenant != "" {
			caller = clusterobjects.CallerFromKube(ctx, kube)
		}
		return clusterobjects.AnnotateNamespaceError(err, ns, tenant, caller)
	}
	return nil
}

func sourceCapsuleLabels(ctx context.Context, kube kubernetes.Interface, ns string) (map[string]string, error) {
	if kube == nil || ns == "" {
		return nil, nil
	}
	src, err := kube.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("source namespace %s: %w", ns, err)
	}
	if src.Labels == nil {
		return nil, nil
	}
	v := src.Labels[clusterobjects.TenantLabel]
	if v == "" {
		return nil, nil
	}
	return map[string]string{clusterobjects.TenantLabel: v}, nil
}

// ensureDestPVC copies the source claim onto dest (same name, remapped
// StorageClass) so ReplicationDestination.spec.restic.destinationPVC exists.
// VolSync does not create that PVC when destinationPVC is set.
func (m Mover) ensureDestPVC(ctx context.Context, namespace, name string) error {
	src, dst := m.Kube, m.destKube()
	if src == nil || dst == nil || src == dst || name == "" {
		return nil
	}
	_, err := dst.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return fmt.Errorf("volsync dest pvc get %s/%s: %w", namespace, name, err)
	}
	srcPVC, err := src.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("volsync dest pvc: source %s/%s: %w", namespace, name, err)
	}
	out := srcPVC.DeepCopy()
	transform.PVC(out, transform.Options{StorageClassMap: m.StorageClassMap})
	out.Name = name
	out.Namespace = namespace
	if out.Labels == nil {
		out.Labels = map[string]string{}
	}
	out.Labels["app.kubernetes.io/managed-by"] = "portage"
	_, err = dst.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, out, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("volsync dest pvc create %s/%s: %w", namespace, name, err)
	}
	return nil
}
