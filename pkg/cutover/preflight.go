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

package cutover

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// Source Postgres restart safety.
//
// Cutover restarts the source (freeze, then unfreeze on rollback or abort).
// On that restart kubelet may re-apply the pod's fsGroup to the data
// volume, OR-ing 0660 onto server.key, and Postgres then refuses to start.
// Marketplace Postgres keeps fsGroup (tenant isolation) with
// fsGroupChangePolicy OnRootMismatch, which is safe only while the volume
// root already matches. Preflight checks that, and that the key is
// acceptable now, before anything is frozen.

// PostgresRestartCheckCmd prints:
//
//	root <mode-octal> <gid>      (omitted when mount is empty)
//	ssl off                      or
//	key <mode-octal> <uid> <path>
//
// or "psql-error <msg>" when the server cannot be queried.
func PostgresRestartCheckCmd(mount string) []string {
	var b strings.Builder
	if mount != "" {
		fmt.Fprintf(&b, "stat -c 'root %%a %%g' %s || exit 1; ", shellQuote(mount))
	}
	b.WriteString(`S=$(psql -U postgres -tAc 'show ssl' 2>&1) || { echo "psql-error $S"; exit 0; }; `)
	b.WriteString(`[ "$S" = on ] || { echo "ssl off"; exit 0; }; `)
	b.WriteString(`K=$(psql -U postgres -tAc 'show ssl_key_file'); `)
	b.WriteString(`case "$K" in /*) ;; *) K="$(psql -U postgres -tAc 'show data_directory')/$K";; esac; `)
	b.WriteString(`stat -c 'key %a %u %n' "$K"`)
	return []string{"/bin/sh", "-c", b.String()}
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// PolicyRisk is the part of the check that needs no exec: without
// OnRootMismatch, kubelet rewrites the whole volume on every mount.
func PolicyRisk(sc *corev1.PodSecurityContext) string {
	if sc == nil || sc.FSGroup == nil {
		return ""
	}
	if sc.FSGroupChangePolicy == nil || *sc.FSGroupChangePolicy != corev1.FSGroupChangeOnRootMismatch {
		return fmt.Sprintf("fsGroup=%d with fsGroupChangePolicy Always (the default): kubelet re-applies group permissions to the whole volume on every mount, including server.key. Set fsGroupChangePolicy: OnRootMismatch", *sc.FSGroup)
	}
	return ""
}

// RestartRisk parses PostgresRestartCheckCmd output. It returns "" when a
// restart is safe, else the reason. checkRoot is false for subPath mounts,
// where the volume root is not visible in the container.
func RestartRisk(out string, fsGroup int64, checkRoot bool) string {
	var rootSeen, keySeen bool
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.Fields(strings.TrimSpace(line))
		if len(f) == 0 {
			continue
		}
		switch f[0] {
		case "psql-error":
			return "cannot query source Postgres to verify server.key: " + strings.Join(f[1:], " ")
		case "ssl":
			if len(f) == 2 && f[1] == "off" {
				keySeen = true // no key in use
			}
		case "root":
			if len(f) != 3 {
				return "unexpected volume root check output: " + line
			}
			mode, err1 := strconv.ParseUint(f[1], 8, 32)
			gid, err2 := strconv.ParseInt(f[2], 10, 64)
			if err1 != nil || err2 != nil {
				return "unexpected volume root check output: " + line
			}
			rootSeen = true
			// kubelet skipPermissionChange: gid == fsGroup, rwx for group, setgid.
			if gid != fsGroup || mode&0o770 != 0o770 || mode&0o2000 == 0 {
				return fmt.Sprintf("volume root is mode %s gid %d; OnRootMismatch skips only for gid %d with g+rwx and setgid, so kubelet will rewrite server.key on the freeze restart", f[1], gid, fsGroup)
			}
		case "key":
			if len(f) < 4 {
				return "unexpected server.key check output: " + line
			}
			mode, err1 := strconv.ParseUint(f[1], 8, 32)
			uid, err2 := strconv.ParseInt(f[2], 10, 64)
			if err1 != nil || err2 != nil {
				return "unexpected server.key check output: " + line
			}
			keySeen = true
			path := strings.Join(f[3:], " ")
			// Postgres: a root-owned key may be 0640; otherwise 0600.
			bad := mode&0o077 != 0
			if uid == 0 {
				bad = mode&0o137 != 0
			}
			if bad {
				return fmt.Sprintf("%s is mode %s; Postgres refuses group/world access, so the source will not start after the freeze. chmod 600 it", path, f[1])
			}
		}
	}
	if checkRoot && !rootSeen {
		return "could not read the data volume root: " + strings.TrimSpace(out)
	}
	if !keySeen {
		return "could not read server.key permissions: " + strings.TrimSpace(out)
	}
	return ""
}
