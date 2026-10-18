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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/PipeOpsHQ/portage/pkg/objectstore"
)

const (
	rcloneSecretName   = "portage-rclone"
	tlsSecretName      = "portage-rsync-tls"
	resticSecretPrefix = "portage-restic-"
)

// SecretNames used in ReplicationSource/Destination specs.
func SecretNames() (rclone, tls string) { return rcloneSecretName, tlsSecretName }

// ResticSecretName is the per-workload VolSync restic repository secret.
// One ClusterPair bucket is shared. Each workload gets its own key so a
// second namespace does not open the first tenant's repository.
func ResticSecretName(workload string) string {
	return resticSecretPrefix + dns1123(workload)
}

func dns1123(name string) string {
	var b strings.Builder
	prevDash := false
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevDash = false
			continue
		}
		if !prevDash && b.Len() > 0 {
			b.WriteByte('-')
			prevDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "workload"
	}
	max := 253 - len(resticSecretPrefix)
	if len(out) > max {
		out = strings.Trim(out[:max], "-")
		if out == "" {
			out = "workload"
		}
	}
	return out
}

// RcloneINI is the VolSync rclone.conf section (must match rcloneConfigSection).
func RcloneINI(c objectstore.Creds) string {
	ep := c.Endpoint
	provider := "AWS"
	if ep != "" {
		provider = "Minio"
	}
	return fmt.Sprintf(`[rclone]
type = s3
provider = %s
env_auth = false
access_key_id = %s
secret_access_key = %s
endpoint = %s
region = %s
`, provider, c.AccessKey, c.SecretKey, ep, orRegion(c.Region))
}

func orRegion(r string) string {
	if r == "" {
		return "us-east-1"
	}
	return r
}

// EnsureSecrets writes rclone.conf, the per-workload restic repo secret, and
// the rsyncTLS PSK if they do not exist.
func EnsureSecrets(ctx context.Context, kube kubernetes.Interface, namespace string, c objectstore.Creds, destPath, workload string) error {
	if kube == nil || namespace == "" {
		return nil
	}
	if c.AccessKey != "" {
		sec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: rcloneSecretName, Namespace: namespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "portage"}},
			Type:       corev1.SecretTypeOpaque,
			Data:       map[string][]byte{"rclone.conf": []byte(RcloneINI(c))},
		}
		if err := upsertSecret(ctx, kube, sec); err != nil {
			return err
		}
		if err := ensureResticSecret(ctx, kube, namespace, c, destPath, workload); err != nil {
			return err
		}
	}
	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		return err
	}
	tls := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: tlsSecretName, Namespace: namespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "portage"}},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"psk": []byte(hex.EncodeToString(psk))},
	}
	// Do not rotate an existing PSK — VolSync peers would desync.
	_, err := kube.CoreV1().Secrets(namespace).Get(ctx, tlsSecretName, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		_, err = kube.CoreV1().Secrets(namespace).Create(ctx, tls, metav1.CreateOptions{})
	}
	return ignoreExists(err)
}

func upsertSecret(ctx context.Context, kube kubernetes.Interface, sec *corev1.Secret) error {
	_, err := kube.CoreV1().Secrets(sec.Namespace).Create(ctx, sec, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		_, err = kube.CoreV1().Secrets(sec.Namespace).Update(ctx, sec, metav1.UpdateOptions{})
	}
	return err
}

func ensureResticSecret(ctx context.Context, kube kubernetes.Interface, namespace string, c objectstore.Creds, destPath, workload string) error {
	pw := make([]byte, 16)
	if _, err := rand.Read(pw); err != nil {
		return err
	}
	name := ResticSecretName(workload)
	data := map[string][]byte{
		"RESTIC_REPOSITORY":     []byte(c.ResticRepository(destPath)),
		"RESTIC_PASSWORD":       []byte(hex.EncodeToString(pw)),
		"AWS_ACCESS_KEY_ID":     []byte(c.AccessKey),
		"AWS_SECRET_ACCESS_KEY": []byte(c.SecretKey),
		"AWS_DEFAULT_REGION":    []byte(orRegion(c.Region)),
	}
	cur, err := kube.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		// Keep RESTIC_PASSWORD stable so incremental repos stay readable.
		if old := cur.Data["RESTIC_PASSWORD"]; len(old) > 0 {
			data["RESTIC_PASSWORD"] = old
		}
		cur.Data = data
		_, err = kube.CoreV1().Secrets(namespace).Update(ctx, cur, metav1.UpdateOptions{})
		return err
	}
	if !errors.IsNotFound(err) {
		return err
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "portage"}},
		Type:       corev1.SecretTypeOpaque,
		Data:       data,
	}
	_, err = kube.CoreV1().Secrets(namespace).Create(ctx, sec, metav1.CreateOptions{})
	return ignoreExists(err)
}

// CopySecrets clones rclone, the per-workload restic secret, and the
// rsyncTLS PSK from source to dest. Restic passwords and PSKs must match
// across clusters or dest cannot read the source repo / TLS peer.
func CopySecrets(ctx context.Context, src, dst kubernetes.Interface, namespace, workload string) error {
	if src == nil || dst == nil || src == dst || namespace == "" {
		return nil
	}
	for _, name := range []string{rcloneSecretName, ResticSecretName(workload), tlsSecretName} {
		sec, err := src.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		cur, err := dst.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			out := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "portage"}},
				Type:       sec.Type,
				Data:       sec.Data,
			}
			if _, err = dst.CoreV1().Secrets(namespace).Create(ctx, out, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
				return fmt.Errorf("copy %s: %w", name, err)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("copy %s: %w", name, err)
		}
		cur.Data = sec.Data
		if _, err = dst.CoreV1().Secrets(namespace).Update(ctx, cur, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("copy %s: %w", name, err)
		}
	}
	return nil
}

func ignoreExists(err error) error {
	if errors.IsAlreadyExists(err) {
		return nil
	}
	return err
}
