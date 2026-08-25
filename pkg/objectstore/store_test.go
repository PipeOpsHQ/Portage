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

package objectstore

import (
	"context"
	"testing"
)

func TestMemoryRoundTrip(t *testing.T) {
	t.Parallel()
	s := &Memory{}
	if err := s.Put(context.Background(), "ns/pg.sql", []byte("-- dump")); err != nil {
		t.Fatal(err)
	}
	b, err := s.Get(context.Background(), "ns/pg.sql")
	if err != nil || string(b) != "-- dump" {
		t.Fatalf("got %q err=%v", b, err)
	}
}

func TestParseURL(t *testing.T) {
	t.Parallel()
	ep, bkt, pfx := ParseURL("s3://velero-backups/portage/prod")
	if ep != "" || bkt != "velero-backups" || pfx != "portage/prod" {
		t.Fatalf("%q %q %q", ep, bkt, pfx)
	}
}
