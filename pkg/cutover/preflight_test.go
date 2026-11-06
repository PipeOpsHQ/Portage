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
	"os/exec"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

func TestRestartRisk(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		out       string
		checkRoot bool
		want      string // substring; "" means safe
	}{
		{"marketplace safe", "root 2770 0\nkey 600 999 /var/lib/postgresql/data/pgdata/server.key\n", true, ""},
		{"root-owned 0640 key", "root 2770 0\nkey 640 0 /certs/server.key", true, ""},
		{"ssl off", "root 2770 0\nssl off", true, ""},
		{"drill: key rewritten", "root 2770 0\nkey 660 999 /var/lib/postgresql/data/pgdata/server.key", true, "chmod 600"},
		{"root mode mismatch", "root 700 0\nkey 600 999 /k", true, "OnRootMismatch skips only"},
		{"root gid mismatch", "root 2770 999\nkey 600 999 /k", true, "gid 999"},
		{"root no setgid", "root 770 0\nkey 600 999 /k", true, "setgid"},
		{"root-owned group-writable key", "root 2770 0\nkey 660 0 /k", true, "chmod 600"},
		{"subPath skips root", "key 600 70 /k", false, ""},
		{"root required", "key 600 999 /k", true, "volume root"},
		{"psql down", "psql-error could not connect", true, "cannot query"},
		{"no output", "", false, "could not read server.key"},
		{"garbage", "root abc 0\nkey 600 1 /k", true, "unexpected"},
	}
	for _, c := range cases {
		got := RestartRisk(c.out, 0, c.checkRoot)
		if c.want == "" && got != "" {
			t.Errorf("%s: want safe, got %q", c.name, got)
		}
		if c.want != "" && !strings.Contains(got, c.want) {
			t.Errorf("%s: got %q want contains %q", c.name, got, c.want)
		}
	}
}

func TestPolicyRisk(t *testing.T) {
	t.Parallel()
	always := corev1.FSGroupChangeAlways
	onRoot := corev1.FSGroupChangeOnRootMismatch
	if PolicyRisk(nil) != "" || PolicyRisk(&corev1.PodSecurityContext{}) != "" {
		t.Fatal("no fsGroup is no risk")
	}
	if PolicyRisk(&corev1.PodSecurityContext{FSGroup: ptr.To(int64(0))}) == "" {
		t.Fatal("unset policy defaults to Always")
	}
	if PolicyRisk(&corev1.PodSecurityContext{FSGroup: ptr.To(int64(0)), FSGroupChangePolicy: &always}) == "" {
		t.Fatal("Always rewrites every mount")
	}
	if r := PolicyRisk(&corev1.PodSecurityContext{FSGroup: ptr.To(int64(0)), FSGroupChangePolicy: &onRoot}); r != "" {
		t.Fatal(r)
	}
}

func TestPostgresRestartCheckCmdIsValidShell(t *testing.T) {
	t.Parallel()
	for _, mount := range []string{"/var/lib/postgresql/data", "", "/data/it's"} {
		cmd := PostgresRestartCheckCmd(mount)
		if err := exec.Command("sh", "-n", "-c", cmd[2]).Run(); err != nil {
			t.Fatalf("mount %q: %v\n%s", mount, err, cmd[2])
		}
	}
}
