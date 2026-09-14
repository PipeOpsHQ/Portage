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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TransportType is how volume bytes move between clusters.
// +kubebuilder:validation:Enum=Direct;ObjectStore
type TransportType string

const (
	// TransportDirect is rsync/TLS (or equivalent) between mover pods.
	TransportDirect TransportType = "Direct"
	// TransportObjectStore hops through S3-compatible storage (rclone).
	// Use when clusters cannot peer, which is the common multi-cloud case.
	TransportObjectStore TransportType = "ObjectStore"
)

// ClusterRef identifies a Kubernetes cluster the hub can reach.
// Auth is exactly one of kubeconfigSecret, azure, aws, or gcp.
// None of those means the cluster this controller runs on.
type ClusterRef struct {
	// Name is a stable identifier used in logs and status (not a K8s name).
	Name string `json:"name"`

	// Address is how dest movers reach this cluster, e.g. "192.0.2.10:30432"
	// for Postgres WAL. Empty means in-cluster DNS (same cluster only).
	// +optional
	Address string `json:"address,omitempty"`

	// KubeconfigSecret is a Secret containing a kubeconfig used to
	// reach this cluster. Prefer azure/aws/gcp so tokens refresh in-process
	// instead of embedding a static kubeconfig or shipping cloud CLIs.
	// +optional
	KubeconfigSecret *SecretKeyRef `json:"kubeconfigSecret,omitempty"`

	// Azure uses Entra ID (DefaultAzureCredential or a service principal)
	// to reach an AKS API server.
	// +optional
	Azure *AzureAuth `json:"azure,omitempty"`

	// AWS uses IAM (IRSA / instance role / keys, optional AssumeRole)
	// to mint an EKS bearer token.
	// +optional
	AWS *AWSAuth `json:"aws,omitempty"`

	// GCP uses ADC or a service-account JSON key to reach a GKE API server.
	// +optional
	GCP *GCPAuth `json:"gcp,omitempty"`

	// ObjectStore is an optional bucket prefix used as the rclone hop and
	// as the default backup artifact location for this cluster.
	// +optional
	ObjectStore *ObjectStoreRef `json:"objectStore,omitempty"`
}

// AzureAuth authenticates to AKS with Entra ID. The hub should run with
// Azure Workload Identity; CredentialsSecret is only for a service principal.
type AzureAuth struct {
	// ResourceID is the AKS ARM id, e.g.
	// /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.ContainerService/managedClusters/{name}
	ResourceID string `json:"resourceID"`

	// TenantID of the Entra ID tenant. Empty uses AZURE_TENANT_ID / DefaultAzureCredential.
	// +optional
	TenantID string `json:"tenantID,omitempty"`

	// ClientID of the hub workload identity or service principal. Empty uses DefaultAzureCredential.
	// +optional
	ClientID string `json:"clientID,omitempty"`

	// ServerID is the AKS Entra server application ID.
	// Defaults to 6dae42f8-4368-4678-94ff-3960e28e3630.
	// +optional
	ServerID string `json:"serverID,omitempty"`

	// UsePrivateFQDN connects to the private API server FQDN.
	// +optional
	UsePrivateFQDN bool `json:"usePrivateFQDN,omitempty"`

	// CredentialsSecret is a service principal. Keys: tenantID, clientID, clientSecret.
	// Namespace empty means the hub namespace. Empty secret ⇒ DefaultAzureCredential.
	// +optional
	CredentialsSecret *SecretKeyRef `json:"credentialsSecret,omitempty"`
}

// AWSAuth authenticates to EKS with IAM. The hub should run with IRSA or an
// instance role; CredentialsSecret is only for static keys.
type AWSAuth struct {
	// ClusterName is the EKS cluster name (not ARN).
	ClusterName string `json:"clusterName"`

	// Region is the EKS region, e.g. us-east-1.
	Region string `json:"region"`

	// RoleARN is an optional role to AssumeRole before calling EKS/STS.
	// +optional
	RoleARN string `json:"roleARN,omitempty"`

	// Endpoint overrides the API server URL from DescribeCluster (private endpoint).
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// CredentialsSecret holds static keys. Keys: accessKey, secretKey, optional sessionToken
	// (or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY). Empty ⇒ default AWS credential chain.
	// +optional
	CredentialsSecret *SecretKeyRef `json:"credentialsSecret,omitempty"`
}

// GCPAuth authenticates to GKE with Application Default Credentials or a
// service-account JSON key.
type GCPAuth struct {
	// Project is the GCP project id.
	Project string `json:"project"`

	// Location is the region or zone, e.g. us-central1 or us-central1-a.
	Location string `json:"location"`

	// Cluster is the GKE cluster name.
	Cluster string `json:"cluster"`

	// UsePrivateEndpoint talks to the private control-plane IP.
	// +optional
	UsePrivateEndpoint bool `json:"usePrivateEndpoint,omitempty"`

	// CredentialsSecret is a GCP service-account JSON key. Key defaults to "key.json".
	// Empty ⇒ ADC (GKE Workload Identity / GOOGLE_APPLICATION_CREDENTIALS).
	// +optional
	CredentialsSecret *SecretKeyRef `json:"credentialsSecret,omitempty"`
}

// AuthMethods is how many of kubeconfigSecret/azure/aws/gcp are set.
func (r ClusterRef) AuthMethods() int {
	n := 0
	if r.KubeconfigSecret != nil {
		n++
	}
	if r.Azure != nil {
		n++
	}
	if r.AWS != nil {
		n++
	}
	if r.GCP != nil {
		n++
	}
	return n
}

// HasRemoteAuth is true when the hub must build an out-of-cluster client.
func (r ClusterRef) HasRemoteAuth() bool { return r.AuthMethods() > 0 }

// ObjectStoreRef points at an S3-compatible bucket.
type ObjectStoreRef struct {
	// URL is s3://bucket/prefix, gs://bucket/prefix, or an S3-compatible endpoint URL.
	URL string `json:"url"`

	// CredentialsSecret holds access keys. Keys: accessKey, secretKey, optional sessionToken.
	// +optional
	CredentialsSecret *LocalSecretRef `json:"credentialsSecret,omitempty"`

	// Region is required by some providers.
	// +optional
	Region string `json:"region,omitempty"`
}

// SecretKeyRef points at a key in a Secret.
type SecretKeyRef struct {
	Name string `json:"name"`
	// Key defaults to "kubeconfig".
	// +optional
	Key string `json:"key,omitempty"`
	// Namespace of the Secret. Empty means the hub namespace.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// LocalSecretRef points at a Secret by name.
type LocalSecretRef struct {
	Name string `json:"name"`
}

// ClusterPairSpec pairs a source cluster with a destination cluster.
type ClusterPairSpec struct {
	Source      ClusterRef `json:"source"`
	Destination ClusterRef `json:"destination"`

	// Transport selects Direct (cluster-to-cluster) or ObjectStore hop.
	// +kubebuilder:default=ObjectStore
	// +optional
	Transport TransportType `json:"transport,omitempty"`

	// StorageClassMap remaps source StorageClass names to destination names.
	// +optional
	StorageClassMap map[string]string `json:"storageClassMap,omitempty"`

	// SnapshotClassMap remaps VolumeSnapshotClass names across clusters.
	// +optional
	SnapshotClassMap map[string]string `json:"snapshotClassMap,omitempty"`
}

// ClusterPairPhase is a high-level pairing health.
type ClusterPairPhase string

const (
	ClusterPairPending  ClusterPairPhase = "Pending"
	ClusterPairReady    ClusterPairPhase = "Ready"
	ClusterPairDegraded ClusterPairPhase = "Degraded"
	ClusterPairFailed   ClusterPairPhase = "Failed"
)

// ClusterPairStatus is the observed state of a ClusterPair.
type ClusterPairStatus struct {
	// +optional
	Phase ClusterPairPhase `json:"phase,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// SourceReachable is true when the hub can list the source API.
	// +optional
	SourceReachable bool `json:"sourceReachable,omitempty"`
	// DestinationReachable is true when the hub can list the destination API.
	// +optional
	DestinationReachable bool `json:"destinationReachable,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=cpair
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.source.name`
// +kubebuilder:printcolumn:name="Destination",type=string,JSONPath=`.spec.destination.name`
// +kubebuilder:printcolumn:name="Transport",type=string,JSONPath=`.spec.transport`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ClusterPair binds two Kubernetes clusters for replication, restore, and cutover.
type ClusterPair struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterPairSpec   `json:"spec,omitempty"`
	Status ClusterPairStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClusterPairList contains a list of ClusterPair.
type ClusterPairList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterPair `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterPair{}, &ClusterPairList{})
}
