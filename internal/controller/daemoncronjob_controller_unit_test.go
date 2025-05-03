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

package controller

import (
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedulingv1 "github.com/xzzpig/daemon-cronjob/api/v1"
)

func TestNodeMatches(t *testing.T) {
	reconciler := &DaemonCronJobReconciler{}

	testCases := []struct {
		name string
		// 节点相关字段
		nodeName   string
		nodeLabels map[string]string
		nodeTaints []corev1.Taint
		// Pod模板相关字段
		specNodeName     string
		specNodeSelector map[string]string
		specTolerations  []corev1.Toleration
		specNodeAffinity *corev1.NodeAffinity
		// 期望结果
		expectMatch bool
	}{
		{
			name:         "Node name matches successfully",
			nodeName:     "test-node",
			specNodeName: "test-node",
			expectMatch:  true,
		},
		{
			name:         "Node name does not match",
			nodeName:     "test-node",
			specNodeName: "other-node",
			expectMatch:  false,
		},
		{
			name:     "Label selector matches successfully",
			nodeName: "test-node",
			nodeLabels: map[string]string{
				"role": "worker",
				"zone": "east",
			},
			specNodeSelector: map[string]string{
				"role": "worker",
			},
			expectMatch: true,
		},
		{
			name:     "Label selector does not match",
			nodeName: "test-node",
			nodeLabels: map[string]string{
				"role": "worker",
				"zone": "east",
			},
			specNodeSelector: map[string]string{
				"role": "master",
			},
			expectMatch: false,
		},
		{
			name:     "Node affinity matches successfully",
			nodeName: "test-node",
			nodeLabels: map[string]string{
				"role": "worker",
				"zone": "east",
			},
			specNodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "role",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"worker"},
								},
							},
						},
					},
				},
			},
			expectMatch: true,
		},
		{
			name:     "Node affinity does not match",
			nodeName: "test-node",
			nodeLabels: map[string]string{
				"role": "worker",
				"zone": "east",
			},
			specNodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "role",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"master"},
								},
							},
						},
					},
				},
			},
			expectMatch: false,
		},
		{
			name:     "Taint and toleration match successfully",
			nodeName: "tainted-node",
			nodeTaints: []corev1.Taint{
				{
					Key:    "key1",
					Value:  "value1",
					Effect: corev1.TaintEffectNoSchedule,
				},
			},
			specTolerations: []corev1.Toleration{
				{
					Key:      "key1",
					Operator: corev1.TolerationOpExists,
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
			expectMatch: true,
		},
		{
			name:     "Taint and toleration do not match",
			nodeName: "tainted-node",
			nodeTaints: []corev1.Taint{
				{
					Key:    "key1",
					Value:  "value1",
					Effect: corev1.TaintEffectNoSchedule,
				},
			},
			specTolerations: []corev1.Toleration{},
			expectMatch:     false,
		},
		{
			name:     "Multiple conditions match successfully",
			nodeName: "complex-node",
			nodeLabels: map[string]string{
				"role":   "worker",
				"region": "east",
				"type":   "compute",
			},
			nodeTaints: []corev1.Taint{
				{
					Key:    "special",
					Value:  "true",
					Effect: corev1.TaintEffectNoSchedule,
				},
			},
			specNodeSelector: map[string]string{
				"role":   "worker",
				"region": "east",
			},
			specTolerations: []corev1.Toleration{
				{
					Key:      "special",
					Operator: corev1.TolerationOpEqual,
					Value:    "true",
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
			specNodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "type",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"compute"},
								},
							},
						},
					},
				},
			},
			expectMatch: true,
		},
		{
			name:     "Multiple conditions do not match",
			nodeName: "complex-node",
			nodeLabels: map[string]string{
				"role":   "worker",
				"region": "east",
				"type":   "compute",
			},
			nodeTaints: []corev1.Taint{
				{
					Key:    "special",
					Value:  "true",
					Effect: corev1.TaintEffectNoSchedule,
				},
			},
			specNodeSelector: map[string]string{
				"role":   "master", // 这里修改为不匹配的标签
				"region": "east",
			},
			specTolerations: []corev1.Toleration{
				{
					Key:      "special",
					Operator: corev1.TolerationOpEqual,
					Value:    "true",
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
			specNodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "type",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"compute"},
								},
							},
						},
					},
				},
			},
			expectMatch: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 构建节点对象
			node := corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   tc.nodeName,
					Labels: tc.nodeLabels,
				},
			}

			if len(tc.nodeTaints) > 0 {
				node.Spec.Taints = tc.nodeTaints
			}

			// 构建 DaemonCronJobSpec 对象
			podSpec := corev1.PodSpec{
				NodeName:     tc.specNodeName,
				NodeSelector: tc.specNodeSelector,
				Tolerations:  tc.specTolerations,
			}

			if tc.specNodeAffinity != nil {
				podSpec.Affinity = &corev1.Affinity{
					NodeAffinity: tc.specNodeAffinity,
				}
			}

			spec := schedulingv1.DaemonCronJobSpec{
				CronJobSpec: batchv1.CronJobSpec{
					JobTemplate: batchv1.JobTemplateSpec{
						Spec: batchv1.JobSpec{
							Template: corev1.PodTemplateSpec{
								Spec: podSpec,
							},
						},
					},
				},
			}

			// 测试匹配情况
			result := reconciler.nodeMatches(node, &spec)
			actualMatch := result == ""

			if actualMatch != tc.expectMatch {
				t.Errorf("nodeMatches() match result = %v, 期望 %v, 错误消息 = %q", actualMatch, tc.expectMatch, result)
			}

			// 如果预期匹配成功，确保返回空字符串
			if tc.expectMatch && result != "" {
				t.Errorf("nodeMatches() 期望空字符串，但返回 %q", result)
			}

			// 如果预期匹配失败，确保返回非空字符串
			if !tc.expectMatch && result == "" {
				t.Errorf("nodeMatches() 期望非空错误消息，但返回空字符串")
			}
		})
	}
}

func TestUpdateStatusCounters(t *testing.T) {
	testCases := []struct {
		name         string
		nodeStatuses []schedulingv1.NodeStatus
		wantCurrent  int32
		wantDesired  int32
	}{
		{
			name:         "Empty node status list",
			nodeStatuses: []schedulingv1.NodeStatus{},
			wantCurrent:  0,
			wantDesired:  0,
		},
		{
			name: "All nodes matched",
			nodeStatuses: []schedulingv1.NodeStatus{
				{NodeName: "node-1", State: schedulingv1.NodeStateMatched},
				{NodeName: "node-2", State: schedulingv1.NodeStateMatched},
				{NodeName: "node-3", State: schedulingv1.NodeStateMatched},
			},
			wantCurrent: 3,
			wantDesired: 3,
		},
		{
			name: "All nodes not matched",
			nodeStatuses: []schedulingv1.NodeStatus{
				{NodeName: "node-1", State: schedulingv1.NodeStateNotMatched},
				{NodeName: "node-2", State: schedulingv1.NodeStateNotMatched},
				{NodeName: "node-3", State: schedulingv1.NodeStateNotMatched},
			},
			wantCurrent: 0,
			wantDesired: 0,
		},
		{
			name: "All nodes pending",
			nodeStatuses: []schedulingv1.NodeStatus{
				{NodeName: "node-1", State: schedulingv1.NodeStatePending},
				{NodeName: "node-2", State: schedulingv1.NodeStatePending},
				{NodeName: "node-3", State: schedulingv1.NodeStatePending},
			},
			wantCurrent: 0,
			wantDesired: 3,
		},
		{
			name: "Mixed states",
			nodeStatuses: []schedulingv1.NodeStatus{
				{NodeName: "node-1", State: schedulingv1.NodeStateMatched},
				{NodeName: "node-2", State: schedulingv1.NodeStateNotMatched},
				{NodeName: "node-3", State: schedulingv1.NodeStatePending},
				{NodeName: "node-4", State: schedulingv1.NodeStateMatched},
				{NodeName: "node-5", State: schedulingv1.NodeStatePending},
			},
			wantCurrent: 2,
			wantDesired: 4,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建测试对象
			dcj := &schedulingv1.DaemonCronJob{
				Status: schedulingv1.DaemonCronJobStatus{
					NodeStatuses: tc.nodeStatuses,
				},
			}

			// 执行被测函数
			updateStatusCounters(dcj)

			// 验证结果
			if dcj.Status.CurrentNodes != tc.wantCurrent {
				t.Errorf("CurrentNodes = %v, 期望值 %v", dcj.Status.CurrentNodes, tc.wantCurrent)
			}

			if dcj.Status.DesiredNodes != tc.wantDesired {
				t.Errorf("DesiredNodes = %v, 期望值 %v", dcj.Status.DesiredNodes, tc.wantDesired)
			}
		})
	}
}
