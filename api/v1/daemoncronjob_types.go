/*
Copyright 2025.

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

package v1

import (
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NodeState represents the state of a node
// +kubebuilder:validation:Enum=Pending;NotMatched;Matched
type NodeState string

const (
	NodeStatePending    NodeState = "Pending"    // Indicates the node needs to be evaluated for conditions
	NodeStateNotMatched NodeState = "NotMatched" // Indicates the node does not meet the selection criteria
	NodeStateMatched    NodeState = "Matched"    // Indicates the node meets the selection criteria and a CronJob has been created/maintained
)

// NodeStatus defines the status of a node
type NodeStatus struct {
	// Node name
	NodeName string `json:"nodeName"`
	// Node state
	// +kubebuilder:validation:Enum=Pending;NotMatched;Matched
	State NodeState `json:"state"`
	// Generation of the DaemonCronJob
	Generation int64 `json:"generation,omitempty"`
	// Time of the last state transition
	LastTransitionTime metav1.Time `json:"lastTransitionTime"`
	// Associated CronJob name
	CronJobName string `json:"cronJobName,omitempty"`
	// Last schedule time
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`
	// Last successful execution time
	LastSuccessfulTime *metav1.Time `json:"lastSuccessfulTime,omitempty"`
	// Error message, empty if the operation is successful, otherwise records specific errors
	Message string `json:"message,omitempty"`
}

// DaemonCronJobSpec defines the desired state of DaemonCronJob
type DaemonCronJobSpec struct {
	// Inline all fields of CronJobSpec
	batchv1.CronJobSpec `json:",inline"`
}

// DaemonCronJobStatus defines the observed state of DaemonCronJob
type DaemonCronJobStatus struct {
	// Current configuration version of DaemonCronJob (e.g., resourceVersion or hash value)
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Node status tracking
	NodeStatuses []NodeStatus `json:"nodeStatuses,omitempty"`

	// Summary status
	// Number of currently matched nodes that have CronJobs running on them
	CurrentNodes int32 `json:"currentNodes"`

	// Number of desired nodes (all nodes that should match the selection criteria)
	DesiredNodes int32 `json:"desiredNodes"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=dcj
// +kubebuilder:printcolumn:name="SCHEDULE",type="string",JSONPath=".spec.schedule"
// +kubebuilder:printcolumn:name="MATCHED",type="integer",JSONPath=".status.currentNodes"
// +kubebuilder:printcolumn:name="DESIRED",type="integer",JSONPath=".status.desiredNodes"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"

// DaemonCronJob is the schema definition for the daemoncronjobs API
type DaemonCronJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DaemonCronJobSpec   `json:"spec,omitempty"`
	Status DaemonCronJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DaemonCronJobList contains a list of DaemonCronJob
type DaemonCronJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DaemonCronJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DaemonCronJob{}, &DaemonCronJobList{})
}
