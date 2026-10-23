#!/usr/bin/env bash
set -e

echo "Deleting kafka-learning namespace and all associated resources..."
kubectl delete namespace kafka-learning --wait=true || true
echo "Teardown complete."
