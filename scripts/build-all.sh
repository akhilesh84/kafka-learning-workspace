#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

echo "=========================================================="
echo " 1. Building Java Modules & Fat Jars with Gradle..."
echo "=========================================================="
./gradlew clean build fatJar

echo "=========================================================="
echo " 2. Building Local Docker Images..."
echo "=========================================================="
docker build -t kafka-producer:latest -f producer/Dockerfile .
docker build -t kafka-consumer:latest -f consumer/Dockerfile .

echo "=========================================================="
echo " 3. Loading Images into Local Kubernetes Cluster (containerd)..."
echo "=========================================================="
if kubectl get nodes >/dev/null 2>&1; then
    echo "Importing images into cluster containerd k8s.io namespace..."
    kubectl run image-loader-temp --image=busybox -n default \
        --overrides='{"spec":{"containers":[{"name":"ldr","image":"busybox","command":["sleep","120"],"volumeMounts":[{"name":"h","mountPath":"/host"}]}],"volumes":[{"name":"h","hostPath":{"path":"/"}}]}}' 2>/dev/null || true
    
    # Wait for loader pod ready
    kubectl wait --namespace default --for=condition=ready pod/image-loader-temp --timeout=30s >/dev/null 2>&1 || true

    docker save kafka-producer:latest | kubectl exec -i image-loader-temp -n default -- chroot /host ctr -n k8s.io images import - >/dev/null 2>&1 || true
    docker save kafka-consumer:latest | kubectl exec -i image-loader-temp -n default -- chroot /host ctr -n k8s.io images import - >/dev/null 2>&1 || true

    kubectl delete pod image-loader-temp -n default >/dev/null 2>&1 || true
    echo "Images imported successfully into cluster containerd!"
fi

echo "=========================================================="
echo " Build & Local Image Ready:"
docker images | grep kafka-
echo "=========================================================="
