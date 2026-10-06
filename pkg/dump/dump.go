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

// Package dump captures application-consistent stdout dumps and restores
// them via exec. This is the portable cloud-hop path (CSI snapshots are not).
package dump

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/kubeexec"
	"github.com/PipeOpsHQ/portage/pkg/objectstore"
	"github.com/PipeOpsHQ/portage/pkg/usefulness"
	"github.com/PipeOpsHQ/portage/pkg/workloads"
)

// Command is the in-pod dump (stdout). Empty means this engine is FS/CSI only.
// Credentials come from the container environment (PipeOps marketplace images
// set POSTGRES_USER/POSTGRES_DB and REDIS_PASSWORD, not the engine defaults).
func Command(engine string) []string {
	switch engine {
	case "postgres", "timescale":
		return []string{"/bin/sh", "-c", postgresDumpScript}
	case "mysql", "mariadb":
		return []string{"/bin/sh", "-c", mysqlDumpScript}
	case "mongo":
		return []string{"/bin/sh", "-c", mongoDumpScript}
	case "redis", "valkey", "keydb", "dragonfly":
		return []string{"/bin/sh", "-c", redisDumpScript}
	default:
		return nil
	}
}

// RestoreCommand reads dump bytes on stdin.
func RestoreCommand(engine string) []string {
	switch engine {
	case "postgres", "timescale":
		return []string{"/bin/sh", "-c", postgresRestoreScript}
	case "mysql", "mariadb":
		return []string{"/bin/sh", "-c", mysqlRestoreScript}
	case "mongo":
		return []string{"/bin/sh", "-c", `mongorestore --archive --gzip`}
	default:
		return nil
	}
}

// Marketplace images set POSTGRES_USER / POSTGRES_DB (often not "postgres").
const postgresDumpScript = `pg_dump -U "${POSTGRES_USER:-postgres}" --no-owner --no-acl "${POSTGRES_DB:-postgres}"`

const postgresRestoreScript = `psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-postgres}" -v ON_ERROR_STOP=1`

const mysqlDumpScript = `export MYSQL_PWD="${MYSQL_PASSWORD:-${MYSQL_PWD:-}}"
mysqldump -u "${MYSQL_USER:-root}" --all-databases --single-transaction`

const mysqlRestoreScript = `export MYSQL_PWD="${MYSQL_PASSWORD:-${MYSQL_PWD:-}}"
mysql -u "${MYSQL_USER:-root}"`

const mongoDumpScript = `mongodump --archive --gzip ${MONGO_INITDB_ROOT_USERNAME:+-u "$MONGO_INITDB_ROOT_USERNAME"} ${MONGO_INITDB_ROOT_PASSWORD:+-p "$MONGO_INITDB_ROOT_PASSWORD"} --authenticationDatabase "${MONGO_INITDB_DATABASE:-admin}"`

// redis-cli --rdb /dev/stdout transfers the RDB then fsyncs the pipe and
// exits 1 ("Fail to fsync '/dev/stdout': Invalid argument"). Write a file,
// stream it, and use the file dump's exit code.
const redisDumpScript = `f=/tmp/portage-rdb-$$
REDISCLI_AUTH="${REDIS_PASSWORD:-${REDISCLI_AUTH:-}}" redis-cli --rdb "$f"
e=$?
if [ "$e" -eq 0 ]; then cat "$f"; fi
rm -f "$f"
exit "$e"`

// Capture execs dump on source and stores it. Returns key, size, useful.
func Capture(ctx context.Context, kube kubernetes.Interface, exec kubeexec.Interface, store objectstore.Store, w classify.Workload, now time.Time) (key string, size int64, useful bool, err error) {
	cmd := Command(w.Engine)
	if len(cmd) == 0 || exec == nil || store == nil {
		return "", 0, false, nil
	}
	pod, container, err := workloads.FirstReadyPod(ctx, kube, w)
	if err != nil {
		return "", 0, false, err
	}
	res, err := exec.Exec(ctx, w.Namespace, pod.Name, container, cmd)
	if err != nil {
		return "", 0, false, fmt.Errorf("dump %s: %w", w.Key(), err)
	}
	data := []byte(res.Stdout)
	size = int64(len(data))
	art := usefulness.ForWorkload(w, usefulness.Input{SizeBytes: size, Files: []usefulness.File{{Path: "dump.sql", SizeBytes: size}}})
	key = objectstore.Key(w.Namespace, w.Key(), now.UTC().Format("20060102T150405Z"))
	if err := store.Put(ctx, key, data); err != nil {
		return key, size, art.Useful, err
	}
	return key, size, art.Useful, nil
}

// Apply downloads the dump and execs restore on dest.
func Apply(ctx context.Context, kube kubernetes.Interface, exec kubeexec.Interface, store objectstore.Store, w classify.Workload, key string) error {
	cmd := RestoreCommand(w.Engine)
	if len(cmd) == 0 || key == "" {
		return nil
	}
	data, err := store.Get(ctx, key)
	if err != nil {
		return err
	}
	pod, container, err := workloads.FirstReadyPod(ctx, kube, w)
	if err != nil {
		return err
	}
	_, err = exec.ExecIn(ctx, w.Namespace, pod.Name, container, cmd, data)
	if err != nil {
		return fmt.Errorf("restore dump %s: %w", w.Key(), err)
	}
	return nil
}
