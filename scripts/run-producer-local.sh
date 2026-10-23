#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export TOPIC_NAME="market-prices"
export POLL_INTERVAL_SECONDS="3"
export SYMBOLS="BTC-USD,ETH-USD,SOL-USD"
export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318/v1/metrics"

./gradlew :producer:bootRun
