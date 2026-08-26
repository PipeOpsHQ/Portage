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
type ClusterRef struct {
	// Name is a stable identifier used in logs and status (not a K8s name).
	Name string `json:"name"`

	// Address is how dest movers reach this cluster, e.g. "192.0.2.10:30432"
	// for Postgres WAL. Empty means in-cluster DNS (same cluster only).
	// +optional
	Address string `json:"address,omitempty"`

	// KubeconfigSecret is a Secret containing a kubeconfig used to
	// reach this cluster. Empty means "the cluster this controller runs on".
	// +optional
	KubeconfigSecret *SecretKeyRef `json:"kubeconfigSecret,omitempty"`

	// ObjectStore is an optional bucket prefix used as the rclone hop and
	// as the default backup artifact location for this cluster.
	// +optional
	ObjectStore *ObjectStoreRef `json:"objectStore,omitempty"`
}

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
