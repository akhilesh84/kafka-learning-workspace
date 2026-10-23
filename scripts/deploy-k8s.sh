#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

echo "=========================================================="
echo " Deploying Kafka Learning Stack to Kubernetes..."
echo "=========================================================="

kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-kafka-kraft.yaml
kubectl apply -f k8s/02-kafka-ui.yaml
kubectl apply -f k8s/03-loki.yaml
kubectl apply -f k8s/04-otel-collector.yaml
kubectl apply -f k8s/05-prometheus.yaml
kubectl apply -f k8s/06-grafana.yaml
kubectl apply -f k8s/07-configmap.yaml
kubectl apply -f k8s/08-consumer-deployment.yaml
kubectl apply -f k8s/09-producer-deployment.yaml

echo "Waiting for core services to start up..."
kubectl rollout status deployment/kafka -n kafka-learning --timeout=120s
kubectl rollout status deployment/loki -n kafka-learning --timeout=60s
kubectl rollout status deployment/otel-collector -n kafka-learning --timeout=60s
kubectl rollout status deployment/prometheus -n kafka-learning --timeout=60s
kubectl rollout status deployment/grafana -n kafka-learning --timeout=60s
kubectl rollout status deployment/kafka-ui -n kafka-learning --timeout=60s

echo ""
echo "All pods are up and running!"
kubectl get pods -n kafka-learning -o wide

echo ""
echo "=========================================================="
echo " Service Access URLs (Run these port-forward commands):"
echo "=========================================================="
echo " 1. Kafka UI:    kubectl port-forward -n kafka-learning svc/kafka-ui 8080:8080"
echo "    -> Open http://localhost:8080"
echo " 2. Grafana:     kubectl port-forward -n kafka-learning svc/grafana 3000:3000"
echo "    -> Open http://localhost:3000 (Loki & Prometheus are pre-configured!)"
echo " 3. Prometheus:  kubectl port-forward -n kafka-learning svc/prometheus 9090:9090"
echo "    -> Open http://localhost:9090"
echo ""
echo " To tail Producer logs: kubectl logs -n kafka-learning -l app=kafka-producer -f"
echo " To tail Consumer logs: kubectl logs -n kafka-learning -l app=kafka-consumer -f"
echo " To scale Consumers:    kubectl scale deployment kafka-consumer -n kafka-learning --replicas=3"
echo "=========================================================="
