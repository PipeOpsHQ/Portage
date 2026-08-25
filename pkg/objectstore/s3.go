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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Creds are static S3-compatible keys (AWS, MinIO, R2, GCS HMAC).
type Creds struct {
	AccessKey string
	SecretKey string
	Session   string
	Region    string
	Endpoint  string
	Bucket    string
	Prefix    string
}

// S3 is a SigV4 object store. Works with AWS and path-style MinIO/R2 endpoints.
type S3 struct {
	API    *s3.Client
	Bucket string
	Prefix string
}

// ParseURL splits s3://bucket/prefix or https://host/bucket/prefix.
func ParseURL(raw string) (endpoint, bucket, prefix string) {
	raw = strings.TrimSpace(raw)
	switch {
	case strings.HasPrefix(raw, "s3://"):
		rest := strings.TrimPrefix(raw, "s3://")
		i := strings.IndexByte(rest, '/')
		if i < 0 {
			return "", rest, ""
		}
		return "", rest[:i], strings.Trim(rest[i+1:], "/")
	case strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://"):
		u, err := url.Parse(raw)
		if err != nil {
			return raw, "", ""
		}
		path := strings.Trim(u.Path, "/")
		i := strings.IndexByte(path, '/')
		ep := u.Scheme + "://" + u.Host
		if i < 0 {
			return ep, path, ""
		}
		return ep, path[:i], path[i+1:]
	default:
		return "", raw, ""
	}
}

// NewS3 builds a SigV4 client. Endpoint empty means AWS default.
func NewS3(ctx context.Context, c Creds) (*S3, error) {
	if c.Bucket == "" {
		return nil, fmt.Errorf("s3: bucket required")
	}
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(c.Region),
	}
	if c.AccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, c.Session),
		))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	s3opts := []func(*s3.Options){}
	if c.Endpoint != "" {
		ep := c.Endpoint
		s3opts = append(s3opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(ep)
			o.UsePathStyle = true
		})
	}
	return &S3{API: s3.NewFromConfig(cfg, s3opts...), Bucket: c.Bucket, Prefix: strings.Trim(c.Prefix, "/")}, nil
}

func (s *S3) key(k string) string {
	k = strings.TrimPrefix(k, "/")
	if s.Prefix == "" {
		return k
	}
	return s.Prefix + "/" + k
}

func (s *S3) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.API.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.Bucket,
		Key:    aws.String(s.key(key)),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("s3 put %s: %w", key, err)
	}
	return nil
}

func (s *S3) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.API.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.Bucket,
		Key:    aws.String(s.key(key)),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 get %s: %w", key, err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// FromEnv builds a Store: Dir, SigV4 S3, or Memory.
func FromEnv() Store {
	if d := os.Getenv("PORTAGE_STORE_DIR"); d != "" {
		return Dir{Root: d}
	}
	c := CredsFromEnv()
	if c.Bucket == "" {
		return &Memory{}
	}
	s, err := NewS3(context.Background(), c)
	if err != nil {
		return &Memory{}
	}
	return s
}

// CredsFromEnv reads AWS_* and PORTAGE_S3_* .
func CredsFromEnv() Creds {
	ep := os.Getenv("PORTAGE_S3_ENDPOINT")
	bkt := os.Getenv("PORTAGE_S3_BUCKET")
	pfx := os.Getenv("PORTAGE_S3_PREFIX")
	if bkt == "" {
		if u := os.Getenv("PORTAGE_S3_URL"); u != "" {
			ep2, b, p := ParseURL(u)
			if ep == "" {
				ep = ep2
			}
			bkt, pfx = b, p
		}
	}
	ak := os.Getenv("AWS_ACCESS_KEY_ID")
	if ak == "" {
		ak = os.Getenv("PORTAGE_S3_ACCESS_KEY")
	}
	sk := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if sk == "" {
		sk = os.Getenv("PORTAGE_S3_SECRET_KEY")
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = os.Getenv("PORTAGE_S3_REGION")
	}
	return Creds{
		AccessKey: ak,
		SecretKey: sk,
		Session:   os.Getenv("AWS_SESSION_TOKEN"),
		Region:    region,
		Endpoint:  ep,
		Bucket:    bkt,
		Prefix:    pfx,
	}
}
