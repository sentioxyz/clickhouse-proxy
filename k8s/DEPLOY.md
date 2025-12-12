# Kubernetes Deployment Guide

This document describes how to deploy ClickHouse Proxy in a Kubernetes cluster.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Deployment Architecture](#deployment-architecture)
- [Quick Deployment](#quick-deployment)
- [Detailed Configuration](#detailed-configuration)
- [Test Environment Deployment](#test-environment-deployment)
- [Troubleshooting](#troubleshooting)

## Prerequisites

1. Kubernetes cluster configured (version >= 1.20)
2. `clickhouse` namespace created:
   ```bash
   kubectl create namespace clickhouse
   ```
3. Private image registry access configured (if using private images)
4. `kubectl` command-line tool installed

## Deployment Architecture

The project includes the following Kubernetes resources:

- **StatefulSet**: Test environment deployment (`test-clickhouse-proxy-test-part-a/` and `test-clickhouse-proxy-test-part-b/`)

## Quick Deployment

### 1. Deploy Main Service

```bash
# Deploy Deployment
kubectl apply -f k8s/deployment.yaml

# Check deployment status
kubectl get deployment -n clickhouse clickhouse-proxy-test

# View Pod status
kubectl get pods -n clickhouse -l app=clickhouse-proxy-test
```

### 2. Deploy Network Policy (Optional)

```bash
kubectl apply -f k8s/networkpolicy.yaml
```

### 3. Deploy Horizontal Pod Autoscaler (Optional)

```bash
kubectl apply -f k8s/hpa.yaml

# View HPA status
kubectl get hpa -n clickhouse clickhouse-proxy-hpa
```

## Detailed Configuration

### Deployment Configuration

File location: `k8s/deployment.yaml`

#### Key Configuration Items

- **Namespace**: `clickhouse`
- **Replicas**: 1 (can be automatically adjusted by HPA)
- **Image**: `us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-proxy:v0.1.0-test`
- **Port**: 9000 (TCP)
- **Resource Limits**:
  - CPU: request 100m, limit 1000m
  - Memory: request 128Mi, limit 512Mi

#### Security Configuration

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  fsGroup: 1000
```

#### Health Checks

- **Liveness Probe**: TCP port check, initial delay 10 seconds, check every 10 seconds
- **Readiness Probe**: TCP port check, initial delay 5 seconds, check every 5 seconds

#### Modify Image Address

Edit `deployment.yaml` and modify the following line:

```yaml
image: your-registry/clickhouse-proxy:your-tag
```

#### Configure Private Image Registry Access

If using a private image registry, create a Secret:

```bash
kubectl create secret docker-registry docker-registry-secret \
  --docker-server=us-west1-docker.pkg.dev \
  --docker-username=your-username \
  --docker-password=your-password \
  --docker-email=your-email \
  -n clickhouse
```

Then uncomment in `deployment.yaml`:

```yaml
imagePullSecrets:
- name: docker-registry-secret
```

### NetworkPolicy Configuration

File location: `k8s/networkpolicy.yaml`

#### Network Policy Rules

- **Ingress**: Allow all Pods to access the proxy's 9000 port
- **Egress**:
  - Allow access to ClickHouse Service (port 9000)
  - Allow DNS queries (kube-system namespace, UDP port 53)

#### Notes

- Ensure ClickHouse Pod labels are `app: clickhouse` (or adjust as needed)
- If NetworkPolicy is not enabled in the cluster, this configuration will be ignored

### HPA Configuration

File location: `k8s/hpa.yaml`

#### Scaling Strategy

- **Minimum replicas**: 2
- **Maximum replicas**: 10
- **CPU threshold**: Scale up when average utilization exceeds 70%
- **Memory threshold**: Scale up when average utilization exceeds 80%

#### Scaling Behavior

- **Scale down**: No scale down again within 5 minutes, maximum 50% reduction per time
- **Scale up**: No stabilization window, maximum 100% increase or add 2 Pods per time (whichever is larger)

#### Prerequisites

Ensure the cluster has Metrics Server installed:

```bash
kubectl top nodes
kubectl top pods
```

## Test Environment Deployment

The project includes complete configurations for two test environments:

### Test Environment A

Directory: `k8s/test-clickhouse-proxy-test-part-a/`

```bash
# Deploy ConfigMap
kubectl apply -f k8s/test-clickhouse-proxy-test-part-a/configmap.yaml

# Deploy Service
kubectl apply -f k8s/test-clickhouse-proxy-test-part-a/service.yaml

# Deploy StatefulSet
kubectl apply -f k8s/test-clickhouse-proxy-test-part-a/statefulset.yaml
```

### Test Environment B

Directory: `k8s/test-clickhouse-proxy-test-part-b/`

```bash
# Deploy ConfigMap
kubectl apply -f k8s/test-clickhouse-proxy-test-part-b/configmap.yaml

# Deploy Service
kubectl apply -f k8s/test-clickhouse-proxy-test-part-b/service.yaml

# Deploy StatefulSet
kubectl apply -f k8s/test-clickhouse-proxy-test-part-b/statefulset.yaml
```

### Test Environment Configuration Details

#### ConfigMap

Configuration file mounted to `/app/config.json` inside the container via ConfigMap:

- **Environment A**: Upstream points to `test-clickhouse-test-part-a:9000`
- **Environment B**: Upstream points to `test-clickhouse-test-part-b:9000`

#### Service

Both environments use Headless Service (`clusterIP: None`), suitable for StatefulSet.

#### StatefulSet

- **Replicas**: 1
- **Image**: `us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-proxy:v0.1.0`
- **Config mount**: Mount configuration file via ConfigMap

## Troubleshooting

### Check Pod Status

```bash
# View all Pods
kubectl get pods -n clickhouse

# View Pod detailed information
kubectl describe pod <pod-name> -n clickhouse

# View Pod logs
kubectl logs <pod-name> -n clickhouse

# View logs in real-time
kubectl logs -f <pod-name> -n clickhouse
```

### Check Deployment Status

```bash
# View Deployment
kubectl get deployment -n clickhouse

# View detailed information
kubectl describe deployment -n clickhouse clickhouse-proxy-test

# View Deployment events
kubectl get events -n clickhouse --field-selector involvedObject.name=clickhouse-proxy-test
```

### Check Service

```bash
# View Service
kubectl get svc -n clickhouse

# View detailed information
kubectl describe svc -n clickhouse <service-name>

# Test Service connection
kubectl run -it --rm debug --image=busybox --restart=Never -n clickhouse -- wget -O- <service-name>:9000
```

### Check ConfigMap

```bash
# View ConfigMap
kubectl get configmap -n clickhouse

# View configuration content
kubectl get configmap <configmap-name> -n clickhouse -o yaml
```

### Common Issues

#### 1. Pod Cannot Start

- Check if image exists and is accessible
- Check if configuration file is correct
- View Pod logs and events

#### 2. Cannot Connect to Upstream ClickHouse

- Check if upstream service address is correct
- Check if network policy allows access
- Verify upstream service is running normally

## Update Deployment

### Update Image Version

```bash
# Method 1: Use kubectl set image
kubectl set image deployment/clickhouse-proxy-test \
  proxy=us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-proxy:v0.2.0 \
  -n clickhouse

# Method 2: Re-apply after editing deployment.yaml
kubectl apply -f k8s/deployment.yaml
```

### Update Configuration

```bash
# Update ConfigMap
kubectl apply -f k8s/test-clickhouse-proxy-test-part-a/configmap.yaml

# Restart Pod to make configuration effective
kubectl rollout restart statefulset/test-clickhouse-proxy-test-part-a -n clickhouse
```

## Clean Up Resources

### Delete Test Environment

```bash
# Delete environment A
kubectl delete -f k8s/test-clickhouse-proxy-test-part-a/

# Delete environment B
kubectl delete -f k8s/test-clickhouse-proxy-test-part-b/
```
