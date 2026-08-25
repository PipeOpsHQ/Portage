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

// ActionType is the kind of mobility operation.
// +kubebuilder:validation:Enum=Backup;Restore;Replicate;Cutover
type ActionType string

const (
	ActionBackup    ActionType = "Backup"
	ActionRestore   ActionType = "Restore"
	ActionReplicate ActionType = "Replicate"
	ActionCutover   ActionType = "Cutover"
)

// ActionPhase is the state machine. Succeeded is only valid after Ready+probe.
type ActionPhase string

const (
	ActionPhasePending      ActionPhase = "Pending"
	ActionPhasePreflight    ActionPhase = "Preflight"
	ActionPhaseQuiescing    ActionPhase = "Quiescing"
	ActionPhaseRehydrating  ActionPhase = "Rehydrating"
	ActionPhaseApplying     ActionPhase = "Applying"
	ActionPhaseWaitingReady ActionPhase = "WaitingReady"
	ActionPhaseHealing      ActionPhase = "Healing"
	ActionPhaseCatchingUp   ActionPhase = "CatchingUp"
	ActionPhasePromoting    ActionPhase = "Promoting"
	ActionPhaseSwitching    ActionPhase = "Switching"
	ActionPhaseAttesting    ActionPhase = "Attesting"
	ActionPhaseSucceeded    ActionPhase = "Succeeded"
	ActionPhaseFailed       ActionPhase = "Failed"
	ActionPhaseRolledBack   ActionPhase = "RolledBack"
)

// ActionSpec is a single run of backup, restore, replicate, or cutover.
type ActionSpec struct {
	// Type of action.
	Type ActionType `json:"type"`

	// PolicyRef is the namespaced Policy this action executes.
	PolicyRef string `json:"policyRef"`

	// FreezeTimeout bounds source write-pause during Restore/Cutover.
	// +optional
	FreezeTimeout string `json:"freezeTimeout,omitempty"`

	// SnapshotID optionally pins restore to a specific artifact.
	// +optional
	SnapshotID string `json:"snapshotID,omitempty"`

	// DryRun runs preflight and classification only.
	// +optional
	DryRun bool `json:"dryRun,omitempty"`

	// Rollback unfreezes source and fires the traffic rollback hook.
	// +optional
	Rollback bool `json:"rollback,omitempty"`
}

// WorkloadActionStatus is per-workload progress inside an Action.
type WorkloadActionStatus struct {
	Name string `json:"name"`
	// Key is namespace/kind/name.
	// +optional
	Key   string        `json:"key,omitempty"`
	Class WorkloadClass `json:"class"`
	// +optional
	Engine string `json:"engine,omitempty"`
	// Ready is Kubernetes Ready (Deployment/STS available, Pod Ready).
	Ready bool `json:"ready"`
	// Probe is the class-specific data probe (pg_isready, PING, HTTP, …).
	// Empty until the probe has run.
	// +optional
	Probe string `json:"probe,omitempty"`
	// ProbeOK is true only when the class probe passed. Action must not
	// Succeeded while any stateful workload has ProbeOK=false.
	// +optional
	ProbeOK bool `json:"probeOK,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	Healed []string `json:"healed,omitempty"`
}

// Attestation is the signed-ish record that an Action actually worked.
type Attestation struct {
	// RecordedAt is when probes passed.
	RecordedAt metav1.Time `json:"recordedAt"`
	// Source is which component wrote this (controller, cli, webhook).
	Source string `json:"source"`
	// Workloads lists probe results copied at success time.
	// +optional
	Workloads []WorkloadActionStatus `json:"workloads,omitempty"`
	// ArtifactIDs are restic snapshot IDs, VolSync latestImage, dump hashes, …
	// +optional
	ArtifactIDs []string `json:"artifactIDs,omitempty"`
}

// ActionStatus is observed execution.
type ActionStatus struct {
	// +optional
	Phase ActionPhase `json:"phase,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	Workloads []WorkloadActionStatus `json:"workloads,omitempty"`
	// +optional
	Attestation *Attestation `json:"attestation,omitempty"`
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=pact
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Policy",type=string,JSONPath=`.spec.policyRef`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Action is one execution of a Policy (backup, restore, replicate, or cutover).
// Succeeded is only written after every stateful workload is Ready and its
// class probe passed. Mover CR success is not sufficient.
type Action struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ActionSpec   `json:"spec,omitempty"`
	Status ActionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ActionList contains a list of Action.
type ActionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Action `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Action{}, &ActionList{})
}
