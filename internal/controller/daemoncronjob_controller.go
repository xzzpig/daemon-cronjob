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
	"context"
	"fmt"
	"reflect"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedulingv1 "github.com/xzzpig/daemon-cronjob/api/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
)

const (
	ownerReferencesField = ".metadata.ownerReferences"     // 用于查找 DaemonCronJob 创建的 CronJob
	nodeNameField        = ".status.nodeStatuses.nodeName" // 用于记录 DaemonCronJob 与 Node 的关联
)

// DaemonCronJobReconciler reconciles a DaemonCronJob object
type DaemonCronJobReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=scheduling.xzzpig.com,resources=daemoncronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=scheduling.xzzpig.com,resources=daemoncronjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=scheduling.xzzpig.com,resources=daemoncronjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// Reconcile is part of the Kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state specified by the user.
func (r *DaemonCronJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("daemoncronjob", req.NamespacedName)
	logger.V(4).Info("Starting reconciliation for DaemonCronJob")

	// 获取 DaemonCronJob 资源
	var daemonCronJob schedulingv1.DaemonCronJob
	if err := r.Get(ctx, req.NamespacedName, &daemonCronJob); err != nil {
		if apierrors.IsNotFound(err) {
			// 资源未找到，可能已被删除
			logger.V(4).Info("DaemonCronJob resource not found, possibly deleted")
			return ctrl.Result{}, nil
		}
		// 获取资源失败，重新排队
		logger.Error(err, "Failed to get DaemonCronJob")
		return ctrl.Result{}, err
	}

	// 设置观察到的生成版本
	if daemonCronJob.Status.ObservedGeneration != daemonCronJob.Generation {
		logger.Info("Detected resource version change, updating all node statuses to Pending",
			"oldGeneration", daemonCronJob.Status.ObservedGeneration,
			"newGeneration", daemonCronJob.Generation)

		daemonCronJob.Status.ObservedGeneration = daemonCronJob.Generation

		// 如果版本变更，将所有节点状态设置为 Pending
		for i := range daemonCronJob.Status.NodeStatuses {
			nodeStatus := &daemonCronJob.Status.NodeStatuses[i]
			nodeStatus.State = schedulingv1.NodeStatePending
			nodeStatus.Generation = daemonCronJob.Generation
			nodeStatus.LastTransitionTime = metav1.Now()
		}

		// 立即更新资源状态
		updateStatusCounters(&daemonCronJob)
		if err := r.Status().Update(ctx, &daemonCronJob); err != nil {
			logger.Error(err, "Failed to update DaemonCronJob status after version change")
			return ctrl.Result{}, err
		}

		// 立即重新触发协调，确保使用最新的状态
		logger.Info("Status updated for version change, triggering re-reconciliation")
		r.Recorder.Eventf(&daemonCronJob, corev1.EventTypeNormal, "VersionChanged",
			"DaemonCronJob version changed, re-evaluating all nodes")
		return ctrl.Result{Requeue: true}, nil
	}

	// 获取所有节点
	var nodeList corev1.NodeList
	if err := r.List(ctx, &nodeList); err != nil {
		logger.Error(err, "Failed to list nodes")
		return ctrl.Result{}, err
	}
	nodeStatusMap := make(map[string]*schedulingv1.NodeStatus)
	for i := range daemonCronJob.Status.NodeStatuses {
		nodeStatus := &daemonCronJob.Status.NodeStatuses[i]
		nodeStatusMap[nodeStatus.NodeName] = nodeStatus
	}

	// 获取现有的 CronJob
	var cronJobList batchv1.CronJobList
	if err := r.List(ctx, &cronJobList, client.InNamespace(req.Namespace), client.MatchingFields{ownerReferencesField: string(daemonCronJob.UID)}); err != nil {
		logger.Error(err, "Failed to list CronJobs")
		return ctrl.Result{}, err
	}
	cronJobMap := make(map[string]*batchv1.CronJob)
	for i := range cronJobList.Items {
		cronJob := &cronJobList.Items[i]
		cronJobMap[cronJob.Name] = cronJob
	}

	// 记录已处理的节点名，用于后续清理不存在节点的状态
	processedNodes := make(map[string]bool)

	// 遍历所有节点
	for _, node := range nodeList.Items {
		processedNodes[node.Name] = true

		// 查找或创建节点状态
		nodeStatus := nodeStatusMap[node.Name]

		if nodeStatus == nil {
			// 如果节点状态不存在，创建一个新的
			daemonCronJob.Status.NodeStatuses = append(daemonCronJob.Status.NodeStatuses, schedulingv1.NodeStatus{
				NodeName:           node.Name,
				State:              schedulingv1.NodeStatePending,
				Generation:         daemonCronJob.Generation,
				LastTransitionTime: metav1.Now(),
			})
			nodeStatus = &daemonCronJob.Status.NodeStatuses[len(daemonCronJob.Status.NodeStatuses)-1]
		}

		// 如果ResourceVersion不一致，更新为Pending状态等待处理
		if nodeStatus.Generation != daemonCronJob.Generation {
			nodeStatus.State = schedulingv1.NodeStatePending
			nodeStatus.Generation = daemonCronJob.Generation
			nodeStatus.LastTransitionTime = metav1.Now()
		}

		// 跳过非Pending状态的节点
		if nodeStatus.State != schedulingv1.NodeStatePending {
			continue
		}

		// 评估节点是否满足条件
		mismatchReason := r.nodeMatches(node, &daemonCronJob.Spec)
		if mismatchReason == "" {
			// 节点匹配，创建或更新CronJob
			cronJobName := fmt.Sprintf("%s-%s", daemonCronJob.Name, node.Name)
			if err := r.createOrUpdateCronJob(ctx, &daemonCronJob, &node, cronJobName); err != nil {
				// 如果创建/更新失败，保持 Pending 状态，记录错误信息
				nodeStatus.Message = fmt.Sprintf("Failed to create/update CronJob: %v", err)
				logger.Error(err, "Failed to create or update CronJob for node", "node", node.Name)
				r.Recorder.Eventf(&daemonCronJob, corev1.EventTypeWarning, "CronJobCreationFailed",
					"Failed to create/update CronJob %s for node %s: %v", cronJobName, node.Name, err)
				continue
			}

			// 更新节点状态为Matched
			nodeStatus.State = schedulingv1.NodeStateMatched
			nodeStatus.CronJobName = cronJobName
			nodeStatus.Message = ""
			nodeStatus.LastTransitionTime = metav1.Now()

			r.Recorder.Eventf(&daemonCronJob, corev1.EventTypeNormal, "CronJobCreated",
				"Created or updated CronJob %s for node %s", cronJobName, node.Name)
		} else {
			// 节点不匹配，删除已存在的CronJob
			cronJobName := fmt.Sprintf("%s-%s", daemonCronJob.Name, node.Name)
			cronJob, found := cronJobMap[cronJobName]
			if found {
				if err := r.Delete(ctx, cronJob); err != nil && !apierrors.IsNotFound(err) {
					nodeStatus.Message = fmt.Sprintf("Failed to delete CronJob: %v", err)
					logger.Error(err, "Failed to delete CronJob", "cronJob", cronJobName)
					continue
				}
				r.Recorder.Eventf(&daemonCronJob, corev1.EventTypeNormal, "CronJobDeleted",
					"Deleted CronJob %s from node %s", cronJobName, node.Name)
			}

			// 更新节点状态为NotMatched
			nodeStatus.State = schedulingv1.NodeStateNotMatched
			nodeStatus.CronJobName = ""
			nodeStatus.LastScheduleTime = nil
			nodeStatus.LastSuccessfulTime = nil
			nodeStatus.Message = mismatchReason
			nodeStatus.LastTransitionTime = metav1.Now()

			if found {
				logger.V(4).Info("Deleted CronJob on node because node no longer matches", "node", node.Name, "cronJob", cronJobName)
			}
		}
	}

	// 清理不存在的节点状态和CronJob
	var updatedNodeStatuses []schedulingv1.NodeStatus
	for _, status := range daemonCronJob.Status.NodeStatuses {
		if processedNodes[status.NodeName] {
			updatedNodeStatuses = append(updatedNodeStatuses, status)
		} else {
			// 删除不存在节点的CronJob
			cronJobName := fmt.Sprintf("%s-%s", daemonCronJob.Name, status.NodeName)
			cronJob := cronJobMap[cronJobName]
			if cronJob != nil {
				if err := r.Delete(ctx, cronJob); err != nil && !apierrors.IsNotFound(err) {
					logger.Error(err, "Failed to delete CronJob for non-existent node", "cronJob", cronJobName)
					// 如果删除失败，暂时保留状态以便下次重试
					updatedNodeStatuses = append(updatedNodeStatuses, status)
				} else {
					logger.Info("Deleted CronJob for non-existent node", "node", status.NodeName, "cronJob", cronJobName)
					r.Recorder.Eventf(&daemonCronJob, corev1.EventTypeNormal, "CronJobDeleted",
						"Deleted CronJob %s for non-existent node %s", cronJobName, status.NodeName)
				}
			}
		}
	}
	daemonCronJob.Status.NodeStatuses = updatedNodeStatuses

	// 更新汇总状态
	updateStatusCounters(&daemonCronJob)

	// 更新资源状态
	if err := r.Status().Update(ctx, &daemonCronJob); err != nil {
		logger.Error(err, "Failed to update DaemonCronJob status")
		return ctrl.Result{}, err
	}

	// 如果有Pending状态的节点，触发重新协调
	for _, status := range daemonCronJob.Status.NodeStatuses {
		if status.State == schedulingv1.NodeStatePending {
			logger.Info("Pending node exists, requeueing after 10 seconds", "node", status.NodeName)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
	}

	logger.V(4).Info("Finished reconciliation for DaemonCronJob")
	return ctrl.Result{}, nil
}

// nodeMatches 检查节点是否满足选择条件，返回空字符串表示匹配成功，否则返回失败原因
func (r *DaemonCronJobReconciler) nodeMatches(node corev1.Node, daemonCronJobSpec *schedulingv1.DaemonCronJobSpec) string {
	logger := log.FromContext(context.Background()).WithValues("node", node.Name)

	if daemonCronJobSpec.JobTemplate.Spec.Template.Spec.NodeName != "" {
		// 如果指定了节点名，直接匹配
		if daemonCronJobSpec.JobTemplate.Spec.Template.Spec.NodeName == node.Name {
			logger.V(4).Info("Node name matches")
			return ""
		}
		logger.V(4).Info("Node name does not match")
		return fmt.Sprintf("Node name '%s' does not match required name '%s'",
			node.Name, daemonCronJobSpec.JobTemplate.Spec.Template.Spec.NodeName)
	}

	// 检查 NodeSelector
	if len(daemonCronJobSpec.JobTemplate.Spec.Template.Spec.NodeSelector) > 0 {
		nodeSelector := daemonCronJobSpec.JobTemplate.Spec.Template.Spec.NodeSelector
		if !labels.SelectorFromSet(nodeSelector).Matches(labels.Set(node.Labels)) {
			logger.V(4).Info("Node labels do not match nodeSelector")
			return fmt.Sprintf("Node labels %v do not match required nodeSelector %v",
				node.Labels, nodeSelector)
		}
	}

	// 检查节点亲和性
	affinity := daemonCronJobSpec.JobTemplate.Spec.Template.Spec.Affinity
	if affinity != nil {
		fakePod := &corev1.Pod{Spec: daemonCronJobSpec.JobTemplate.Spec.Template.Spec}
		match, _ := nodeaffinity.GetRequiredNodeAffinity(fakePod).Match(&node)
		if !match {
			logger.V(4).Info("Node does not meet required affinity selector terms")
			return "Node does not meet required node affinity selector terms"
		}
	}

	// 检查节点污点是否被容忍
	if len(node.Spec.Taints) > 0 {
		for _, taint := range node.Spec.Taints {
			isTaintTolerated := false
			for _, tolerations := range daemonCronJobSpec.JobTemplate.Spec.Template.Spec.Tolerations {
				if tolerations.ToleratesTaint(&taint) {
					isTaintTolerated = true
					break
				}
			}
			if !isTaintTolerated {
				logger.V(4).Info("Node taint is not tolerated", "taint", taint)
				return fmt.Sprintf("Node taint %v is not tolerated", taint)
			}
		}
	}

	// 所有条件均满足
	return ""
}

// createOrUpdateCronJob 创建或更新节点对应的 CronJob
func (r *DaemonCronJobReconciler) createOrUpdateCronJob(ctx context.Context, dcj *schedulingv1.DaemonCronJob, node *corev1.Node, cronJobName string) error {
	logger := log.FromContext(ctx)

	cronJobSpec := dcj.Spec.CronJobSpec.DeepCopy()

	// 确保Pod运行在指定节点上，并清除可能冲突的调度设置
	cronJobSpec.JobTemplate.Spec.Template.Spec.NodeName = node.Name
	cronJobSpec.JobTemplate.Spec.Template.Spec.NodeSelector = nil
	cronJobSpec.JobTemplate.Spec.Template.Spec.Affinity = nil
	cronJobSpec.JobTemplate.Spec.Template.Spec.Tolerations = nil

	// 准备CronJob资源
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cronJobName,
			Namespace: dcj.Namespace,
		},
		Spec: *cronJobSpec,
	}
	if err := ctrl.SetControllerReference(dcj, cronJob, r.Scheme); err != nil {
		logger.Error(err, "Failed to set controller reference for CronJob")
		return err
	}

	// 尝试获取现有CronJob
	var existingCronJob batchv1.CronJob
	err := r.Get(ctx, types.NamespacedName{Name: cronJobName, Namespace: dcj.Namespace}, &existingCronJob)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// 创建新的 CronJob
			logger.Info("Creating new CronJob", "name", cronJobName)
			return r.Create(ctx, cronJob)
		}
		return err
	}

	// 检查 CronJob 是否需要更新
	if !reflect.DeepEqual(existingCronJob.Spec, cronJob.Spec) ||
		!reflect.DeepEqual(existingCronJob.Labels, cronJob.Labels) {
		// 更新现有的 CronJob
		existingCronJob.Spec = cronJob.Spec
		existingCronJob.Labels = cronJob.Labels
		logger.Info("Updating existing CronJob", "name", cronJobName)
		return r.Update(ctx, &existingCronJob)
	}

	logger.V(4).Info("CronJob already up to date", "name", cronJobName)
	return nil
}

// updateStatusCounters 更新 DaemonCronJob 状态中的计数器
func updateStatusCounters(daemonCronJob *schedulingv1.DaemonCronJob) {
	currentNodes := 0
	desiredNodes := 0

	// 遍历所有节点状态
	for _, nodeStatus := range daemonCronJob.Status.NodeStatuses {
		// 计算已匹配节点数
		if nodeStatus.State == schedulingv1.NodeStateMatched {
			currentNodes++
		}

		// 计算期望节点数 (包括已匹配和待匹配的节点)
		if nodeStatus.State == schedulingv1.NodeStateMatched ||
			nodeStatus.State == schedulingv1.NodeStatePending {
			desiredNodes++
		}
	}

	// 更新状态计数器
	daemonCronJob.Status.CurrentNodes = int32(currentNodes)
	daemonCronJob.Status.DesiredNodes = int32(desiredNodes)
}

// SetupWithManager 设置控制器与Manager的关系
func (r *DaemonCronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// 创建索引器，用于快速查找与特定节点相关的 DaemonCronJob
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &schedulingv1.DaemonCronJob{}, nodeNameField, func(obj client.Object) []string {
		dcj, ok := obj.(*schedulingv1.DaemonCronJob)
		if !ok {
			return nil
		}

		nodeNames := make([]string, len(dcj.Status.NodeStatuses))
		for i, status := range dcj.Status.NodeStatuses {
			nodeNames[i] = status.NodeName
		}
		return nodeNames
	}); err != nil {
		return err
	}

	// 设置 OwnerReference 索引，用于快速查找属于 DaemonCronJob 的 CronJob
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &batchv1.CronJob{}, ownerReferencesField, func(obj client.Object) []string {
		ownerRefs := obj.GetOwnerReferences()
		owners := make([]string, len(ownerRefs))
		for i, refs := range ownerRefs {
			owners[i] = string(refs.UID)
		}
		return owners
	}); err != nil {
		return err
	}

	// 创建节点事件处理器
	nodeHandler := handler.EnqueueRequestsFromMapFunc(
		func(ctx context.Context, obj client.Object) []reconcile.Request {
			logger := log.FromContext(ctx)
			node, ok := obj.(*corev1.Node)
			if !ok {
				logger.Error(fmt.Errorf("expected Node object, got %T", obj), "unable to process non-Node object")
				return nil
			}

			// 检查节点是否是首次创建
			isNewNode := isNodeFirstCreated(node)
			logger.V(4).Info("Node change detected", "node", node.Name, "isNewNode", isNewNode)

			// 如果是首次创建或节点被删除，触发所有DaemonCronJob的重新协调
			if isNewNode || obj.GetDeletionTimestamp() != nil {
				logger.V(4).Info("New node detected or node deleted, triggering reconciliation for all DaemonCronJobs", "node", node.Name)

				var allDcjList schedulingv1.DaemonCronJobList
				if err := r.List(ctx, &allDcjList); err != nil {
					logger.Error(err, "Failed to list all DaemonCronJobs")
					return nil
				}

				// 为所有 DaemonCronJob 创建协调请求
				var requests []reconcile.Request
				for i := range allDcjList.Items {
					dcj := &allDcjList.Items[i]
					logger.V(4).Info("Triggering reconciliation for DaemonCronJob due to new/deleted node", "node", node.Name, "daemoncronjob", dcj.Name)
					requests = append(requests, reconcile.Request{
						NamespacedName: types.NamespacedName{
							Name:      dcj.Name,
							Namespace: dcj.Namespace,
						},
					})
				}
				return requests
			}
			// 如果节点不是新创建的，使用索引查找与该节点关联的 DaemonCronJob
			logger.V(4).Info("Node updated, finding relevant DaemonCronJobs", "node", node.Name)

			// 使用索引查找与该节点关联的 DaemonCronJob
			var dcjList schedulingv1.DaemonCronJobList
			if err := r.List(ctx, &dcjList, client.MatchingFields{nodeNameField: node.Name}); err != nil {
				logger.Error(err, "Failed to find relevant DaemonCronJobs by index")
				return nil
			}

			// 为每个关联的 DaemonCronJob 添加协调请求
			var requests []reconcile.Request
			for _, dcj := range dcjList.Items {
				logger.V(4).Info("Found DaemonCronJob associated with node", "node", node.Name, "daemoncronjob", dcj.Name)

				// 将节点状态设置为 Pending，以触发重新评估
				for i, status := range dcj.Status.NodeStatuses {
					if status.NodeName == node.Name {
						logger.V(4).Info("Updating node status to Pending", "node", node.Name, "daemoncronjob", dcj.Name)
						dcj.Status.NodeStatuses[i].State = schedulingv1.NodeStatePending
						dcj.Status.NodeStatuses[i].Generation = dcj.Generation // 更新版本号
						dcj.Status.NodeStatuses[i].LastTransitionTime = metav1.Now()

						// 尝试更新状态
						updateStatusCounters(&dcj)
						if err := r.Status().Update(ctx, &dcj); err != nil {
							logger.Error(err, "Failed to update node status to Pending", "node", node.Name, "daemoncronjob", dcj.Name)
						}
						break
					}
				}

				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Name:      dcj.Name,
						Namespace: dcj.Namespace,
					},
				})
			}
			return requests
		},
	)

	// 用于检测节点是否是首次创建
	nodePredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			// 节点创建事件总是允许
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			// 节点更新时检查 ResourceVersion、Labels 或 Taints 是否变化
			oldNode, okOld := e.ObjectOld.(*corev1.Node)
			newNode, okNew := e.ObjectNew.(*corev1.Node)
			if !okOld || !okNew {
				return false // 类型不匹配，忽略
			}
			// 仅关心 ResourceVersion、Labels 和 Taints 的变化
			return oldNode.GetResourceVersion() != newNode.GetResourceVersion() ||
				!reflect.DeepEqual(oldNode.Labels, newNode.Labels) ||
				!reflect.DeepEqual(oldNode.Spec.Taints, newNode.Spec.Taints)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			// 节点删除时触发处理
			return true
		},
		GenericFunc: func(e event.GenericEvent) bool {
			// 不处理 Generic 事件
			return false
		},
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&schedulingv1.DaemonCronJob{}).
		Watches(
			&corev1.Node{},
			nodeHandler,
			builder.WithPredicates(nodePredicate),
		).
		// Owns(&batchv1.CronJob{}).
		Named("daemoncronjob").
		Complete(r)
}

// isNodeFirstCreated 检查节点是否是首次创建
// 通过判断节点的创建时间与当前时间的差距来确定
// 如果节点创建时间距离当前时间小于一定阈值，则认为是新节点
func isNodeFirstCreated(node *corev1.Node) bool {
	// 创建时间在5分钟内的节点视为新创建的节点
	threshold := 5 * time.Minute
	return time.Since(node.CreationTimestamp.Time) <= threshold
}
