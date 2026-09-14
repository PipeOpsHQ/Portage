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
	"fmt"
	"regexp"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice/v6"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
)

// AKS Entra ID server application (public, documented by Azure).
const aksAADServerID = "6dae42f8-4368-4678-94ff-3960e28e3630"

var aksResourceID = regexp.MustCompile(`(?i)^/subscriptions/([^/]+)/resource[gG]roups/([^/]+)/providers/Microsoft\.ContainerService/managedClusters/([^/]+)/?$`)

type aksIDs struct {
	Subscription, ResourceGroup, Cluster string
}

func parseAKSResourceID(id string) (aksIDs, error) {
	m := aksResourceID.FindStringSubmatch(id)
	if m == nil {
		return aksIDs{}, fmt.Errorf("azure.resourceID must be an AKS ARM id, got %q", id)
	}
	return aksIDs{Subscription: m[1], ResourceGroup: m[2], Cluster: m[3]}, nil
}

func (r Resolver) azureREST(ctx context.Context, ref portagev1alpha1.ClusterRef) (*rest.Config, error) {
	a := ref.Azure
	if a == nil || a.ResourceID == "" {
		return nil, fmt.Errorf("cluster %s: azure.resourceID is required", ref.Name)
	}
	ids, err := parseAKSResourceID(a.ResourceID)
	if err != nil {
		return nil, err
	}
	cred, err := r.azureCred(ctx, a)
	if err != nil {
		return nil, err
	}
	client, err := armcontainerservice.NewManagedClustersClient(ids.Subscription, cred, nil)
	if err != nil {
		return nil, err
	}
	cluster, err := client.Get(ctx, ids.ResourceGroup, ids.Cluster, nil)
	if err != nil {
		return nil, fmt.Errorf("aks get %s: %w", a.ResourceID, err)
	}
	host := ""
	if a.UsePrivateFQDN && cluster.Properties != nil && cluster.Properties.PrivateFQDN != nil {
		host = *cluster.Properties.PrivateFQDN
	}
	if host == "" && cluster.Properties != nil && cluster.Properties.Fqdn != nil {
		host = *cluster.Properties.Fqdn
	}
	userCreds, err := client.ListClusterUserCredentials(ctx, ids.ResourceGroup, ids.Cluster, nil)
	if err != nil {
		return nil, fmt.Errorf("aks user credentials: %w", err)
	}
	raw, err := firstKubeconfig(userCreds.Kubeconfigs)
	if err != nil {
		return nil, err
	}
	kc, err := clientcmd.Load(raw)
	if err != nil {
		return nil, fmt.Errorf("aks kubeconfig: %w", err)
	}
	ca, kubeHost, clientCert := kubeconfigTLSFrom(kc)
	if kubeHost != "" && host == "" {
		host = kubeHost
	}
	if clientCert {
		cfg, err := clientcmd.RESTConfigFromKubeConfig(raw)
		if err != nil {
			return nil, err
		}
		if a.UsePrivateFQDN && host != "" {
			cfg.Host = "https://" + host
		}
		return cfg, nil
	}
	serverID := a.ServerID
	if serverID == "" {
		serverID = aksAADServerID
	}
	ts := &cachedToken{
		skew: time.Minute,
		fetch: func(ctx context.Context) (string, time.Time, error) {
			tok, err := cred.GetToken(ctx, policy.TokenRequestOptions{
				Scopes: []string{serverID + "/.default"},
			})
			if err != nil {
				return "", time.Time{}, fmt.Errorf("entra token for AKS: %w", err)
			}
			return tok.Token, tok.ExpiresOn, nil
		},
	}
	return restConfig(host, ca, ts)
}

func (r Resolver) azureCred(ctx context.Context, a *portagev1alpha1.AzureAuth) (azcore.TokenCredential, error) {
	if a.CredentialsSecret != nil {
		data, err := r.credSecret(ctx, a.CredentialsSecret)
		if err != nil {
			return nil, err
		}
		tenant := secretString(data, "tenantID", "AZURE_TENANT_ID")
		clientID := secretString(data, "clientID", "AZURE_CLIENT_ID")
		secret := secretString(data, "clientSecret", "AZURE_CLIENT_SECRET")
		if tenant == "" {
			tenant = a.TenantID
		}
		if clientID == "" {
			clientID = a.ClientID
		}
		if tenant == "" || clientID == "" || secret == "" {
			return nil, fmt.Errorf("azure credentials secret needs tenantID, clientID, clientSecret")
		}
		return azidentity.NewClientSecretCredential(tenant, clientID, secret, nil)
	}
	opts := &azidentity.DefaultAzureCredentialOptions{}
	if a.TenantID != "" {
		opts.TenantID = a.TenantID
	}
	return azidentity.NewDefaultAzureCredential(opts)
}

func firstKubeconfig(cfgs []*armcontainerservice.CredentialResult) ([]byte, error) {
	for _, c := range cfgs {
		if c != nil && len(c.Value) > 0 {
			return c.Value, nil
		}
	}
	return nil, fmt.Errorf("aks user credentials: empty kubeconfig")
}
