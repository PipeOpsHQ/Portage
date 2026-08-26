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

package postgres

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/movers"
)

func TestDiscoverOnlyPostgres(t *testing.T) {
	t.Parallel()
	m := Mover{}
	cap, err := m.Discover(context.Background(), classify.Workload{Engine: "mysql", Class: portagev1alpha1.ClassSQLLogical})
	if err != nil || cap.Replicate {
		t.Fatalf("mysql must not use postgres-streaming: %+v", cap)
	}
	cap, err = m.Discover(context.Background(), classify.Workload{Engine: "postgres", Class: portagev1alpha1.ClassSQLLogical})
	if err != nil || !cap.Replicate {
		t.Fatalf("postgres: %+v err=%v", cap, err)
	}
}

func TestReplicateWritesStandbyConfigOnDest(t *testing.T) {
	t.Parallel()
	dest := k8sfake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}})
	m := Mover{Dest: dest}
	w := classify.Workload{Namespace: "ns", Name: "pg", Engine: "postgres", Class: portagev1alpha1.ClassSQLLogical}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{Name: "aws"}, movers.ClusterHandle{Name: "gcp"}); err != nil {
		t.Fatal(err)
	}
	cm, err := dest.CoreV1().ConfigMaps("ns").Get(context.Background(), "portage-standby-pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cm.Data["hot_standby"] != "on" || cm.Data["primary_conninfo"] == "" {
		t.Fatalf("%v", cm.Data)
	}
	if want := "host=portage-pg-primary.ns.svc port=5432 user=replicator"; !strings.Contains(cm.Data["primary_conninfo"], want) {
		t.Fatalf("in-cluster conninfo=%q", cm.Data["primary_conninfo"])
	}
	if _, err := dest.BatchV1().Jobs("ns").Get(context.Background(), "portage-basebackup-pg", metav1.GetOptions{}); err != nil {
		t.Fatalf("basebackup job: %v", err)
	}
	pr, _ := m.Probe(context.Background(), w, movers.ClusterHandle{Name: "gcp"})
	if pr.OK {
		t.Fatal("probe must wait for pg_basebackup to complete")
	}
}

func TestReplicateUsesSourceAddressForWAL(t *testing.T) {
	t.Parallel()
	src := k8sfake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}})
	dest := k8sfake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}})
	m := Mover{Kube: src, Dest: dest}
	w := classify.Workload{Namespace: "ns", Name: "pg", Engine: "postgres", Class: portagev1alpha1.ClassSQLLogical}
	if err := m.Replicate(context.Background(), w, movers.ClusterHandle{Name: "aws", Address: "172.18.0.1:30432"}, movers.ClusterHandle{Name: "gcp"}); err != nil {
		t.Fatal(err)
	}
	cm, err := dest.CoreV1().ConfigMaps("ns").Get(context.Background(), "portage-standby-pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cm.Data["primary_conninfo"], "host=172.18.0.1 port=30432") {
		t.Fatalf("conninfo=%q", cm.Data["primary_conninfo"])
	}
	svc, err := src.CoreV1().Services("ns").Get(context.Background(), "portage-pg-primary", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if svc.Spec.Type != corev1.ServiceTypeNodePort || svc.Spec.Ports[0].NodePort != 30432 {
		t.Fatalf("service=%+v", svc.Spec)
	}
}

func TestName(t *testing.T) {
	t.Parallel()
	if (Mover{}).Name() != "postgres-streaming" {
		t.Fatal("name")
	}
	_ = movers.Mover(Mover{})
}
