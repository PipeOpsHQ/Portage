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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"k8s.io/client-go/rest"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
)

const gcpCloudScope = "https://www.googleapis.com/auth/cloud-platform"

func gkeResourceName(project, location, cluster string) string {
	return fmt.Sprintf("projects/%s/locations/%s/clusters/%s", project, location, cluster)
}

type gkeCluster struct {
	Endpoint   string `json:"endpoint"`
	MasterAuth *struct {
		ClusterCaCertificate string `json:"clusterCaCertificate"`
	} `json:"masterAuth"`
	PrivateClusterConfig *struct {
		PrivateEndpoint string `json:"privateEndpoint"`
	} `json:"privateClusterConfig"`
}

func (r Resolver) gcpREST(ctx context.Context, ref portagev1alpha1.ClusterRef) (*rest.Config, error) {
	g := ref.GCP
	if g == nil || g.Project == "" || g.Location == "" || g.Cluster == "" {
		return nil, fmt.Errorf("cluster %s: gcp.project, gcp.location, and gcp.cluster are required", ref.Name)
	}
	ts, err := r.gcpTokenSource(ctx, g)
	if err != nil {
		return nil, err
	}
	cl, err := getGKECluster(ctx, ts, gkeResourceName(g.Project, g.Location, g.Cluster))
	if err != nil {
		return nil, err
	}
	host := cl.Endpoint
	if g.UsePrivateEndpoint && cl.PrivateClusterConfig != nil && cl.PrivateClusterConfig.PrivateEndpoint != "" {
		host = cl.PrivateClusterConfig.PrivateEndpoint
	}
	if cl.MasterAuth == nil || cl.MasterAuth.ClusterCaCertificate == "" {
		return nil, fmt.Errorf("gke cluster %s: missing masterAuth.clusterCaCertificate", g.Cluster)
	}
	ca, err := base64.StdEncoding.DecodeString(cl.MasterAuth.ClusterCaCertificate)
	if err != nil {
		return nil, fmt.Errorf("gke CA: %w", err)
	}
	wrapped := &cachedToken{
		skew: time.Minute,
		fetch: func(ctx context.Context) (string, time.Time, error) {
			tok, err := ts.Token()
			if err != nil {
				return "", time.Time{}, err
			}
			exp := tok.Expiry
			if exp.IsZero() {
				exp = time.Now().Add(50 * time.Minute)
			}
			return tok.AccessToken, exp, nil
		},
	}
	return restConfig(host, ca, wrapped)
}

func (r Resolver) gcpTokenSource(ctx context.Context, g *portagev1alpha1.GCPAuth) (oauth2.TokenSource, error) {
	if g.CredentialsSecret == nil {
		c, err := google.FindDefaultCredentials(ctx, gcpCloudScope)
		if err != nil {
			return nil, fmt.Errorf("gcp ADC: %w", err)
		}
		return c.TokenSource, nil
	}
	data, err := r.credSecret(ctx, g.CredentialsSecret)
	if err != nil {
		return nil, err
	}
	raw := data[g.CredentialsSecret.Key]
	if len(raw) == 0 {
		raw = data["key.json"]
	}
	if len(raw) == 0 {
		raw = data["credentials.json"]
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("gcp credentials secret missing key.json")
	}
	creds, err := google.CredentialsFromJSON(ctx, raw, gcpCloudScope)
	if err != nil {
		return nil, fmt.Errorf("gcp credentials json: %w", err)
	}
	return creds.TokenSource, nil
}

func getGKECluster(ctx context.Context, ts oauth2.TokenSource, name string) (*gkeCluster, error) {
	tok, err := ts.Token()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://container.googleapis.com/v1/"+name, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gke get %s: %w", name, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("gke get %s: %s: %s", name, resp.Status, truncate(body, 512))
	}
	cl := &gkeCluster{}
	if err := json.Unmarshal(body, cl); err != nil {
		return nil, fmt.Errorf("gke cluster json: %w", err)
	}
	return cl, nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n])
}
