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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/PipeOpsHQ/portage/pkg/objectstore"
)

func TestEnsureSecretsCreatesRcloneAndPSK(t *testing.T) {
	t.Parallel()
	kube := k8sfake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}})
	c := objectstore.Creds{AccessKey: "ak", SecretKey: "sk", Endpoint: "http://minio:9000", Region: "us-east-1"}
	if err := EnsureSecrets(context.Background(), kube, "ns", c, "s3://portage/files"); err != nil {
		t.Fatal(err)
	}
	rclone, err := kube.CoreV1().Secrets("ns").Get(context.Background(), rcloneSecretName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ini := string(rclone.Data["rclone.conf"])
	if !strings.Contains(ini, "access_key_id = ak") || !strings.Contains(ini, "provider = Minio") {
		t.Fatalf("ini=%s", ini)
	}
	tls, err := kube.CoreV1().Secrets("ns").Get(context.Background(), tlsSecretName, metav1.GetOptions{})
	if err != nil || len(tls.Data["psk"]) < 16 {
		t.Fatalf("psk missing: %v", err)
	}
	// second call must not rotate psk
	psk := string(tls.Data["psk"])
	restic, err := kube.CoreV1().Secrets("ns").Get(context.Background(), resticSecretName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	repo := string(restic.Data["RESTIC_REPOSITORY"])
	if !strings.Contains(repo, "minio:9000") || !strings.Contains(repo, "portage") {
		t.Fatalf("restic repo=%s", repo)
	}
	pw := string(restic.Data["RESTIC_PASSWORD"])
	if err := EnsureSecrets(context.Background(), kube, "ns", c, "s3://portage/files"); err != nil {
		t.Fatal(err)
	}
	tls2, _ := kube.CoreV1().Secrets("ns").Get(context.Background(), tlsSecretName, metav1.GetOptions{})
	if string(tls2.Data["psk"]) != psk {
		t.Fatal("psk rotated")
	}
	restic2, _ := kube.CoreV1().Secrets("ns").Get(context.Background(), resticSecretName, metav1.GetOptions{})
	if string(restic2.Data["RESTIC_PASSWORD"]) != pw {
		t.Fatal("restic password rotated")
	}
}
