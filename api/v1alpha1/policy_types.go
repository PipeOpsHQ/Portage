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

// WorkloadClass is how Portage treats a discovered workload.
// +kubebuilder:validation:Enum=Stateless;GenericPVC;SQLLogical;KVLogical;SearchFS;QueueDurable;ObjectStore;OperatorManaged;UnknownStateful
type WorkloadClass string

const (
	ClassStateless       WorkloadClass = "Stateless"
	ClassGenericPVC      WorkloadClass = "GenericPVC"
	ClassSQLLogical      WorkloadClass = "SQLLogical"
	ClassKVLogical       WorkloadClass = "KVLogical"
	ClassSearchFS        WorkloadClass = "SearchFS"
	ClassQueueDurable    WorkloadClass = "QueueDurable"
	ClassObjectStore     WorkloadClass = "ObjectStore"
	ClassOperatorManaged WorkloadClass = "OperatorManaged"
	ClassUnknownStateful WorkloadClass = "UnknownStateful"
)

// RendererKind selects how destination manifests are produced.
// +kubebuilder:validation:Enum=Sanitize;Git;Webhook
type RendererKind string

const (
	// RendererSanitize clones live source objects and strips cluster-local
	// fields (zone topology, cloud LB annotations, volumeHandles, …).
	// This is the default OSS path and does not require a PaaS.
	RendererSanitize RendererKind = "Sanitize"
	// RendererGit renders from a Git directory (Kustomize/Helm later).
	RendererGit RendererKind = "Git"
	// RendererWebhook asks an external renderer (a PaaS control plane,
	// GitOps engine, or custom operator) to produce dest manifests.
	RendererWebhook RendererKind = "Webhook"
)

// TargetSelector chooses which workloads a Policy covers.
type TargetSelector struct {
	// Namespaces on the source cluster. Empty means the Policy's own namespace
	// mapped onto the source cluster.
	// +optional
	Namespaces []string `json:"namespaces,omitempty"`

	// LabelSelector restricts to objects carrying these labels.
	// +optional
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"`
}

// BackupSpec is the backup half of a Policy. Backup is a safety net, not cutover.
type BackupSpec struct {
	// +kubebuilder:default=true
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// RPO is the target backup interval, e.g. "24h".
	// +optional
	RPO string `json:"rpo,omitempty"`

	// RequireUseful refuses to mark BackupHealthy if artifacts fail usefulness
	// checks (size floor, dump shape). Default true.
	// +kubebuilder:default=true
	// +optional
	RequireUseful bool `json:"requireUseful,omitempty"`
}

// ReplicateSpec is continuous or scheduled volume/app replication to the dest.
type ReplicateSpec struct {
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// RPO is the target lag, e.g. "15m".
	// +optional
	RPO string `json:"rpo,omitempty"`
}

// RestoreSpec controls in-place and remote restore.
type RestoreSpec struct {
	// Auto starts a Restore Action when a covered PVC/namespace is empty and
	// a useful artifact exists. Default false — production must opt in.
	// +optional
	Auto bool `json:"auto,omitempty"`

	// NeverOverwriteNewer refuses restore if live data is newer than the artifact.
	// +kubebuilder:default=true
	// +optional
	NeverOverwriteNewer bool `json:"neverOverwriteNewer,omitempty"`
}

// CutoverSpec controls planned failover / cloud hop.
type CutoverSpec struct {
	// TrafficHook names how to switch ingress/DNS. Empty means no traffic move
	// (data-only promote). Implementations: webhook URL, or in-tree later.
	// +optional
	TrafficHook string `json:"trafficHook,omitempty"`

	// HoldSource is how long to keep the source frozen-but-present for rollback.
	// +optional
	HoldSource string `json:"holdSource,omitempty"`
}

// RendererSpec chooses dest manifest production.
type RendererSpec struct {
	// +kubebuilder:default=Sanitize
	// +optional
	Kind RendererKind `json:"kind,omitempty"`

	// WebhookURL is required when Kind=Webhook.
	// +optional
	WebhookURL string `json:"webhookURL,omitempty"`

	// Git is used when Kind=Git.
	// +optional
	Git *GitSource `json:"git,omitempty"`
}

// GitSource is a Git directory of desired state.
type GitSource struct {
	URL string `json:"url"`
	// +optional
	Ref string `json:"ref,omitempty"`
	// +optional
	Path string `json:"path,omitempty"`
}

// PolicySpec is the desired continuity for a set of workloads.
type PolicySpec struct {
	// ClusterPair is a cluster-scoped pair. Empty means in-cluster
	// (source = destination = local).
	// +optional
	ClusterPair string `json:"clusterPair,omitempty"`

	// +optional
	Selector TargetSelector `json:"selector,omitempty"`

	// +optional
	Backup BackupSpec `json:"backup,omitempty"`

	// +optional
	Replicate ReplicateSpec `json:"replicate,omitempty"`

	// +optional
	Restore RestoreSpec `json:"restore,omitempty"`

	// +optional
	Cutover CutoverSpec `json:"cutover,omitempty"`

	// +optional
	Renderer RendererSpec `json:"renderer,omitempty"`

	// MoverOverrides pins a mover name per WorkloadClass (e.g. SQLLogical: postgres-streaming).
	// +optional
	MoverOverrides map[string]string `json:"moverOverrides,omitempty"`
}

// PolicyPhase is rollup health for a Policy.
type PolicyPhase string

const (
	PolicyPending   PolicyPhase = "Pending"
	PolicyHealthy   PolicyPhase = "Healthy"
	PolicyDegraded  PolicyPhase = "Degraded"
	PolicyUnhealthy PolicyPhase = "Unhealthy"
)

// InventoryItem is one classified workload.
type InventoryItem struct {
	Namespace string        `json:"namespace"`
	Name      string        `json:"name"`
	Kind      string        `json:"kind"`
	Class     WorkloadClass `json:"class"`
	Engine    string        `json:"engine,omitempty"`
	// PVCNames are claims that hold this workload's state.
	// +optional
	PVCNames []string `json:"pvcNames,omitempty"`
	// Mover is the selected mover name, if any.
	// +optional
	Mover string `json:"mover,omitempty"`
	// Unclassified is true when Class=UnknownStateful — still backed up, but noisy.
	// +optional
	Unclassified bool `json:"unclassified,omitempty"`
}

// ArtifactHealth is usefulness of the latest backup for one workload.
type ArtifactHealth struct {
	Workload       string       `json:"workload"`
	Useful         bool         `json:"useful"`
	SizeBytes      int64        `json:"sizeBytes,omitempty"`
	LastBackupTime *metav1.Time `json:"lastBackupTime,omitempty"`
	Message        string       `json:"message,omitempty"`
}

// PolicyStatus is observed inventory and health.
type PolicyStatus struct {
	// +optional
	Phase PolicyPhase `json:"phase,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	Inventory []InventoryItem `json:"inventory,omitempty"`
	// +optional
	Artifacts []ArtifactHealth `json:"artifacts,omitempty"`
	// BackupHealthy is false if any stateful workload lacks a useful artifact.
	// +optional
	BackupHealthy bool `json:"backupHealthy,omitempty"`
	// ReplicaLag is a human-readable lag summary when replicate is enabled.
	// +optional
	ReplicaLag string `json:"replicaLag,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=ppol
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Pair",type=string,JSONPath=`.spec.clusterPair`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Backup",type=boolean,JSONPath=`.status.backupHealthy`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Policy is the desired backup / replicate / restore / cutover contract
// for a set of workloads.
type Policy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PolicySpec   `json:"spec,omitempty"`
	Status PolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PolicyList contains a list of Policy.
type PolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Policy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Policy{}, &PolicyList{})
}
