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

package probe

import (
	"strconv"
	"strings"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
)

// SizeCommand returns an in-pod command that prints a single integer byte
// count of live data. Empty means "no live size probe" (use CSI snapshot only).
func SizeCommand(w classify.Workload) []string {
	switch w.Engine {
	case "postgres", "timescale":
		return []string{"/bin/sh", "-c", postgresSizeScript}
	case "mysql", "mariadb":
		return []string{"/bin/sh", "-c", `du -sb /var/lib/mysql /bitnami/mysql 2>/dev/null | awk '{s+=$1} END {print s+0}'`}
	case "mongo":
		return []string{"/bin/sh", "-c", `du -sb /data/db /bitnami/mongodb 2>/dev/null | awk '{s+=$1} END {print s+0}'`}
	case "redis", "valkey", "keydb", "dragonfly":
		return []string{"/bin/sh", "-c", `du -sb /data /bitnami/redis 2>/dev/null | awk '{s+=$1} END {print s+0}'`}
	}
	switch w.Class {
	case portagev1alpha1.ClassSQLLogical, portagev1alpha1.ClassKVLogical, portagev1alpha1.ClassSearchFS,
		portagev1alpha1.ClassQueueDurable, portagev1alpha1.ClassObjectStore:
		return []string{"/bin/sh", "-c", `du -sb /data /var/lib /bitnami 2>/dev/null | awk '{s+=$1} END {print s+0}'`}
	default:
		return nil
	}
}

// postgresSizeScript prefers dump size (application-consistent usefulness)
// and falls back to PGDATA bytes. Tiny certs-only trees fail the floor.
const postgresSizeScript = `
set +e
if command -v pg_dumpall >/dev/null 2>&1; then
  n=$(pg_dumpall 2>/dev/null | wc -c | tr -d ' ')
  if [ -n "$n" ] && [ "$n" -gt 0 ]; then echo "$n"; exit 0; fi
fi
du -sb "${PGDATA:-/var/lib/postgresql/data}" /var/lib/postgresql /bitnami/postgresql 2>/dev/null | awk '{s+=$1} END {print s+0}'
`

// ParseBytes reads the first integer from command output.
func ParseBytes(stdout string) int64 {
	s := strings.TrimSpace(stdout)
	if s == "" {
		return 0
	}
	// First token, in case of trailing noise.
	if i := strings.IndexAny(s, " \n\t"); i > 0 {
		s = s[:i]
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}
