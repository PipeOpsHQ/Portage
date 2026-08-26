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
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/workloads"
)

const (
	replUser = "replicator"
	replPass = "portage-repl"
	svcName  = "portage-pg-primary"
)

var createReplicatorCmd = []string{"/bin/sh", "-c",
	`psql -U postgres -v ON_ERROR_STOP=1 -c "DO \$\$ BEGIN CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'portage-repl'; EXCEPTION WHEN duplicate_object THEN NULL; END \$\$;"`}

var basebackupCmd = []string{"/bin/sh", "-c", `
set -e
mkdir -p "$PGDATA"
if [ -f "$PGDATA/standby.signal" ] || [ -f "$PGDATA/backup_label" ]; then
  exit 0
fi
pg_basebackup -h "$PRIMARY_HOST" -p "${PRIMARY_PORT:-5432}" -U replicator -D "$PGDATA" -Fp -Xs -P -R
`}

// ensurePrimaryService publishes the source Postgres for dest WAL fetch.
func (m Mover) ensurePrimaryService(ctx context.Context, w classify.Workload, address string) error {
	if m.Kube == nil {
		return nil
	}
	svcType := corev1.ServiceTypeClusterIP
	ports := []corev1.ServicePort{{Name: "pg", Port: 5432, TargetPort: intstr.FromInt(5432)}}
	if address != "" {
		svcType = corev1.ServiceTypeNodePort
		_, p := splitHostPort(address)
		if n, err := strconv.Atoi(p); err == nil && n >= 30000 && n <= 32767 {
			ports[0].NodePort = int32(n)
		}
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: w.Namespace,
			Labels:    map[string]string{"portage.io/name": w.Name, "portage.io/role": "primary"},
		},
		Spec: corev1.ServiceSpec{
			Type: svcType,
			Selector: map[string]string{
				"app": w.Name,
			},
			Ports: ports,
		},
	}
	_, err := m.Kube.CoreV1().Services(w.Namespace).Create(ctx, svc, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (m Mover) ensureReplicatorRole(ctx context.Context, w classify.Workload) error {
	if m.Exec == nil || m.Kube == nil {
		return nil
	}
	pod, container, err := workloads.FirstReadyPod(ctx, m.Kube, w)
	if err != nil {
		return err
	}
	_, err = m.Exec.Exec(ctx, w.Namespace, pod.Name, container, createReplicatorCmd)
	return err
}

func (m Mover) ensureBasebackupJob(ctx context.Context, w classify.Workload, primaryHost, primaryPort string) error {
	dest := m.Dest
	if dest == nil {
		dest = m.Kube
	}
	if dest == nil {
		return fmt.Errorf("postgres-streaming: dest kube required for basebackup")
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "portage-basebackup-" + w.Name,
			Namespace: w.Namespace,
			Labels:    map[string]string{"portage.io/name": w.Name},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptr.To(int32(6)),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{{
						Name:    "basebackup",
						Image:   "postgres:16",
						Command: basebackupCmd,
						Env: []corev1.EnvVar{
							{Name: "PGDATA", Value: "/var/lib/postgresql/data/pgdata"},
							{Name: "PRIMARY_HOST", Value: primaryHost},
							{Name: "PRIMARY_PORT", Value: primaryPort},
							{Name: "PGPASSWORD", Value: replPass},
						},
					}},
				},
			},
		},
	}
	_, err := dest.BatchV1().Jobs(w.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (m Mover) patchDestStandby(ctx context.Context, w classify.Workload) error {
	dest := m.Dest
	if dest == nil {
		dest = m.Kube
	}
	if dest == nil {
		return nil
	}
	sts, err := dest.AppsV1().StatefulSets(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
	if err != nil {
		return nil // dest STS applied later
	}
	if sts.Spec.Template.Annotations == nil {
		sts.Spec.Template.Annotations = map[string]string{}
	}
	sts.Spec.Template.Annotations["portage.io/standby"] = "true"
	addCMVolume(sts, "portage-standby-"+w.Name)
	_, err = dest.AppsV1().StatefulSets(w.Namespace).Update(ctx, sts, metav1.UpdateOptions{})
	return err
}

func addCMVolume(sts *appsv1.StatefulSet, cmName string) {
	for _, v := range sts.Spec.Template.Spec.Volumes {
		if v.Name == "portage-standby" {
			return
		}
	}
	sts.Spec.Template.Spec.Volumes = append(sts.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "portage-standby",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
			},
		},
	})
	if len(sts.Spec.Template.Spec.Containers) == 0 {
		return
	}
	c := &sts.Spec.Template.Spec.Containers[0]
	c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: "portage-standby", MountPath: "/etc/portage-standby", ReadOnly: true})
	c.Env = append(c.Env, corev1.EnvVar{Name: "PRIMARY_CONNINFO_FILE", Value: "/etc/portage-standby/primary_conninfo"})
}
