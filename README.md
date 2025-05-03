# DaemonCronJob

DaemonCronJob is a Kubernetes custom resource controller that combines the functionality of DaemonSet and CronJob:

- Works on multiple nodes in a cluster like a DaemonSet, based on node selectors, affinities, and tolerations
- Executes tasks periodically according to a schedule defined by a cron expression like a CronJob

## Features

- Supports standard cron expressions for defining execution schedules
- Selects target nodes via node selectors, affinity rules, and tolerations
- Creates an individual CronJob for each matching node, ensuring tasks execute on the specified nodes
- Provides detailed status tracking, including execution status for each node
- Automatically cleans up resources when nodes no longer match the selection criteria

## Installation

### Using Helm

```bash
# Add the Helm repository
helm repo add daemon-cronjob https://xzzpig.github.io/daemon-cronjob/
helm repo update

# Install the chart
helm install daemon-cronjob daemon-cronjob/daemon-cronjob
```

### Using Kubernetes Bundles

```bash
# Install using the bundled YAML
kubectl apply -f https://raw.githubusercontent.com/xzzpig/daemon-cronjob/main/dist/install.yaml
```

### Using kubectl

```bash
# Install CRD and controller
kubectl apply -f https://raw.githubusercontent.com/xzzpig/daemon-cronjob/main/config/crd/bases/scheduling.xzzpig.com_daemoncronjobs.yaml
kubectl apply -f https://raw.githubusercontent.com/xzzpig/daemon-cronjob/main/config/rbac
kubectl apply -f https://raw.githubusercontent.com/xzzpig/daemon-cronjob/main/config/manager/manager.yaml
```

### Building from Source

```bash
# Clone the repository
git clone https://github.com/xzzpig/daemon-cronjob.git
cd daemon-cronjob

# Install CRD and deploy controller
make install
make deploy
```

## Usage Example

Here's a simple DaemonCronJob example that performs a system status check on all Linux nodes every 5 minutes:

```yaml
apiVersion: scheduling.xzzpig.com/v1
kind: DaemonCronJob
metadata:
  name: system-status-checker
spec:
  schedule: "*/5 * * * *"
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 1
  concurrencyPolicy: Replace
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: status-checker
            image: busybox:1.35
            command:
            - /bin/sh
            - -c
            - |
              echo "Node status check started at $(date)"
              echo "Hostname: $(hostname)"
              echo "CPU usage: $(top -bn1 | grep 'Cpu(s)' | awk '{print $2 + $4}')%"
              echo "Memory usage:"
              free -m
              echo "Node status check completed at $(date)"
          restartPolicy: OnFailure
  nodeSelector:
    kubernetes.io/os: linux
```

## Advanced Configuration

### Node Selection

DaemonCronJob supports three methods for selecting target nodes:

1. **Node Selector**: Uses simple key-value label selection

```yaml
nodeSelector:
  kubernetes.io/os: linux
  node-role.kubernetes.io/worker: "true"
```

2. **Node Affinity**: Uses more complex selection expressions

```yaml
affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
      - matchExpressions:
        - key: node-role.kubernetes.io/control-plane
          operator: DoesNotExist
```

3. **Tolerations**: Allows tasks to be scheduled on nodes with specific taints

```yaml
tolerations:
- key: node-role.kubernetes.io/control-plane
  operator: Exists
  effect: NoSchedule
```

## Status Monitoring

View the status of a DaemonCronJob:

```bash
kubectl get dcj  # dcj is the short name for DaemonCronJob
```

Example output:

```
NAME                   SCHEDULE      MATCHED   DESIRED   AGE
system-status-checker  */5 * * * *   3         3         10m
```

View detailed status, including per-node status:

```bash
kubectl describe dcj system-status-checker
```

## Troubleshooting

If CronJobs on some nodes aren't being created or are not working properly:

1. Check the DaemonCronJob controller logs:
   ```bash
   kubectl logs -n daemon-cronjob-system deployment/daemon-cronjob-controller-manager manager
   ```

2. Check the DaemonCronJob status:
   ```bash
   kubectl describe dcj <name>
   ```
   Look for node status and error messages

3. Verify node labels and taints:
   ```bash
   kubectl get node <node-name> --show-labels
   kubectl describe node <node-name> | grep Taints
   ```

## License

Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License").

