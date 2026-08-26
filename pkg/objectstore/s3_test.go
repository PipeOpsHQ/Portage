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

import "testing"

func TestCredsFromEnv(t *testing.T) {
	t.Setenv("PORTAGE_S3_ENDPOINT", "http://minio:9000")
	t.Setenv("PORTAGE_S3_BUCKET", "portage")
	t.Setenv("PORTAGE_S3_PREFIX", "tenants")
	t.Setenv("AWS_ACCESS_KEY_ID", "ak")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "sk")
	t.Setenv("AWS_REGION", "us-east-1")
	c := CredsFromEnv()
	if c.Bucket != "portage" || c.AccessKey != "ak" || c.Endpoint != "http://minio:9000" {
		t.Fatalf("%+v", c)
	}
	s, err := NewS3(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	if s.Bucket != "portage" || s.key("a.sql") != "tenants/a.sql" {
		t.Fatalf("prefix key=%s", s.key("a.sql"))
	}
}

func TestResticRepository(t *testing.T) {
	t.Parallel()
	c := Creds{Endpoint: "http://minio:9000", Bucket: "portage"}
	got := c.ResticRepository("s3://portage/files/app")
	if got != "s3:http://minio:9000/portage/files/app" {
		t.Fatalf("got %q", got)
	}
}
