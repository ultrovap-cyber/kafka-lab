#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Я проверяю Docker Desktop до изменения ресурсов кластера.
operating_system=$(docker info --format '{{.OperatingSystem}}')
if [[ "$operating_system" != *"Docker Desktop"* ]]; then
  echo 'Я не обнаружил Docker Desktop в выбранном Docker-контексте.' >&2
  exit 1
fi
kubectl --context docker-desktop cluster-info
kubectl --context docker-desktop get nodes
# Я собираю образ и разворачиваю Kafka в Kubernetes Docker Desktop.
docker build -t kafka-demo:local .
kubectl --context docker-desktop apply -f k8s/kafka-deployment.yml
kubectl --context docker-desktop -n kafka-lab rollout status deployment/zookeeper --timeout=180s
kubectl --context docker-desktop -n kafka-lab rollout status deployment/kafka --timeout=300s
kubectl --context docker-desktop -n kafka-lab exec deployment/kafka -- kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic test-topic --partitions 1 --replication-factor 1
kubectl --context docker-desktop apply -f kafka-demo-deployment.yaml
kubectl --context docker-desktop -n kafka-lab rollout status deployment/kafka-demo --timeout=180s
kubectl --context docker-desktop -n kafka-lab get deployments,pods,services
# Я жду подтверждения обмена, поскольку Running не доказывает доставку сообщения.
for attempt in {1..30}; do
  output=$(kubectl --context docker-desktop -n kafka-lab logs deployment/kafka-demo --tail=100)
  if grep -q 'Проверка: OK' <<< "$output"; then
    printf '%s\n' "$output"
    echo 'Я подтвердил развёртывание и обмен сообщениями в Kubernetes Docker Desktop.'
    exit 0
  fi
  sleep 2
done
echo 'Я не получил подтверждения обмена сообщениями.' >&2
exit 1
