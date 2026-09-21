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

	"github.com/PipeOpsHQ/portage/pkg/transform"
)

func ensureNamespace(ctx context.Context, kube kubernetes.Interface, ns string) error {
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
	_, err = kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{})
	return ignoreExists(err)
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
