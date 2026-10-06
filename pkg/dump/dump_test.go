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

package dump

import (
	"strings"
	"testing"
)

func TestCommandPostgres(t *testing.T) {
	t.Parallel()
	cmd := Command("postgres")
	joined := strings.Join(cmd, " ")
	if len(cmd) == 0 || !strings.Contains(joined, "pg_dump") {
		t.Fatalf("%v", cmd)
	}
	if !strings.Contains(joined, "POSTGRES_USER") || !strings.Contains(joined, "POSTGRES_DB") {
		t.Fatalf("postgres dump must read image env: %v", cmd)
	}
	restore := strings.Join(RestoreCommand("postgres"), " ")
	if !strings.Contains(restore, "POSTGRES_USER") || !strings.Contains(restore, "POSTGRES_DB") {
		t.Fatalf("postgres restore must read image env: %s", restore)
	}
	if RestoreCommand("nginx") != nil {
		t.Fatal("nginx must not have dump restore")
	}
}

func TestCommandRedisWritesFileNotStdout(t *testing.T) {
	t.Parallel()
	cmd := Command("redis")
	joined := strings.Join(cmd, " ")
	if !strings.Contains(joined, "REDIS_PASSWORD") {
		t.Fatalf("redis dump must send REDISCLI_AUTH from REDIS_PASSWORD: %v", cmd)
	}
	if !strings.Contains(joined, "/tmp/portage-rdb-") {
		t.Fatalf("redis dump must write a file then cat it (fsync on /dev/stdout exits 1): %v", cmd)
	}
	if strings.Contains(joined, "--rdb /dev/stdout") {
		t.Fatalf("redis --rdb /dev/stdout fsync-fails the dump: %v", cmd)
	}
}
