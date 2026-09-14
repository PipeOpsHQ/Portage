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

// PluginType is what the webhook implements.
// +kubebuilder:validation:Enum=Mover
type PluginType string

const (
	PluginMover PluginType = "Mover"
)

// PluginSpec registers an out-of-tree backup/restore/replicate engine.
// The hub POSTs JSON to WebhookURL; swapping this CR (or Policy.mover)
// hot-swaps Velero, restic/K8up, etc. without rebuilding the operator.
type PluginSpec struct {
	// Type is Mover (the only type today).
	// +kubebuilder:default=Mover
	// +optional
	Type PluginType `json:"type,omitempty"`

	// WebhookURL is the plugin HTTP endpoint. One URL handles all operations.
	WebhookURL string `json:"webhookURL"`

	// Classes this mover may be selected for. Empty means every class.
	// +optional
	Classes []WorkloadClass `json:"classes,omitempty"`

	// Backup is true if the plugin implements Backup.
	// +optional
	Backup bool `json:"backup,omitempty"`

	// Restore is true if the plugin implements Restore.
	// +optional
	Restore bool `json:"restore,omitempty"`

	// Replicate is true if the plugin implements Replicate.
	// +optional
	Replicate bool `json:"replicate,omitempty"`

	// TimeoutSeconds for one webhook call. Default 120.
	// +optional
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
}

// PluginStatus is observed plugin health.
type PluginStatus struct {
	// +optional
	Ready bool `json:"ready,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=pplug
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="URL",type=string,JSONPath=`.spec.webhookURL`
// +kubebuilder:printcolumn:name="Ready",type=boolean,JSONPath=`.status.ready`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Plugin registers a hot-swappable mover (Velero, restic, …) by webhook URL.
// Policy.spec.backup.mover / restore.mover / replicate.mover / moverOverrides
// select it by metadata.name.
type Plugin struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PluginSpec   `json:"spec,omitempty"`
	Status PluginStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PluginList contains a list of Plugin.
type PluginList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Plugin `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Plugin{}, &PluginList{})
}
