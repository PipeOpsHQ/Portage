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

package clusters

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"k8s.io/client-go/rest"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
)

const (
	eksTokenPrefix   = "k8s-aws-v1."
	eksClusterHeader = "X-K8s-Aws-Id"
	eksPresignSecs   = "60"
)

func (r Resolver) awsREST(ctx context.Context, ref portagev1alpha1.ClusterRef) (*rest.Config, error) {
	a := ref.AWS
	if a == nil || a.ClusterName == "" || a.Region == "" {
		return nil, fmt.Errorf("cluster %s: aws.clusterName and aws.region are required", ref.Name)
	}
	cfg, err := r.awsConfig(ctx, a)
	if err != nil {
		return nil, err
	}
	eksClient := eks.NewFromConfig(cfg)
	out, err := eksClient.DescribeCluster(ctx, &eks.DescribeClusterInput{Name: aws.String(a.ClusterName)})
	if err != nil {
		return nil, fmt.Errorf("eks describe cluster %s: %w", a.ClusterName, err)
	}
	if out.Cluster == nil || out.Cluster.CertificateAuthority == nil || out.Cluster.CertificateAuthority.Data == nil {
		return nil, fmt.Errorf("eks cluster %s: missing certificateAuthority.data", a.ClusterName)
	}
	ca, err := base64.StdEncoding.DecodeString(aws.ToString(out.Cluster.CertificateAuthority.Data))
	if err != nil {
		return nil, fmt.Errorf("eks cluster CA: %w", err)
	}
	host := a.Endpoint
	if host == "" {
		host = aws.ToString(out.Cluster.Endpoint)
	}
	stsClient := sts.NewFromConfig(cfg)
	ts := &cachedToken{
		skew: 15 * time.Second,
		fetch: func(ctx context.Context) (string, time.Time, error) {
			tok, err := eksPresignedToken(ctx, stsClient, a.ClusterName)
			if err != nil {
				return "", time.Time{}, err
			}
			return tok, time.Now().Add(14 * time.Minute), nil
		},
	}
	return restConfig(host, ca, ts)
}

func (r Resolver) awsConfig(ctx context.Context, a *portagev1alpha1.AWSAuth) (aws.Config, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(a.Region),
	}
	if a.CredentialsSecret != nil {
		data, err := r.credSecret(ctx, a.CredentialsSecret)
		if err != nil {
			return aws.Config{}, err
		}
		ak := secretString(data, "accessKey", "AWS_ACCESS_KEY_ID")
		sk := secretString(data, "secretKey", "AWS_SECRET_ACCESS_KEY")
		st := secretString(data, "sessionToken", "AWS_SESSION_TOKEN")
		if ak == "" || sk == "" {
			return aws.Config{}, fmt.Errorf("aws credentials secret missing accessKey/secretKey")
		}
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(ak, sk, st),
		))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, err
	}
	role := a.RoleARN
	if a.CredentialsSecret != nil {
		if data, err := r.credSecret(ctx, a.CredentialsSecret); err == nil {
			if v := secretString(data, "roleARN", "AWS_ROLE_ARN"); v != "" && role == "" {
				role = v
			}
		}
	}
	if role != "" {
		stsClient := sts.NewFromConfig(cfg)
		cfg.Credentials = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(stsClient, role, func(o *stscreds.AssumeRoleOptions) {
			o.RoleSessionName = "portage"
		}))
	}
	return cfg, nil
}

func eksPresignedToken(ctx context.Context, stsClient *sts.Client, clusterName string) (string, error) {
	presign := sts.NewPresignClient(stsClient)
	out, err := presign.PresignGetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}, func(po *sts.PresignOptions) {
		po.ClientOptions = append(po.ClientOptions, func(o *sts.Options) {
			o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
				return stack.Build.Add(&eksClusterHeaderMW{cluster: clusterName}, middleware.After)
			})
		})
	})
	if err != nil {
		return "", fmt.Errorf("sts presign GetCallerIdentity: %w", err)
	}
	return eksTokenFromURL(out.URL), nil
}

func eksTokenFromURL(presigned string) string {
	return eksTokenPrefix + base64.RawURLEncoding.EncodeToString([]byte(presigned))
}

type eksClusterHeaderMW struct{ cluster string }

func (m *eksClusterHeaderMW) ID() string { return "EKSClusterIDHeader" }

func (m *eksClusterHeaderMW) HandleBuild(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (
	middleware.BuildOutput, middleware.Metadata, error,
) {
	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		return middleware.BuildOutput{}, middleware.Metadata{}, fmt.Errorf("eks token: unexpected transport %T", in.Request)
	}
	req.Header.Set(eksClusterHeader, m.cluster)
	req.Header.Set("X-Amz-Expires", eksPresignSecs)
	return next.HandleBuild(ctx, in)
}
