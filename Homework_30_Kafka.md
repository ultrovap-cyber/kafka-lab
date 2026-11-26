# Домашняя работа № 30. Kafka, CI/CD и Kubernetes

## Решение

Я подготовил Python-приложение для отправки и чтения сообщений Kafka, Docker Compose, Dockerfile, конфигурацию GitLab CI/CD и манифесты Kubernetes. Я разместил зависимости в `app/requirements.txt`, а манифест приложения — в корневом файле `kafka-demo-deployment.yaml`.

Я сохранил результаты прежних запусков в `previous-kind-logs/`. Они относятся к прежней версии проекта и кластеру kind; я не использую их как подтверждение работы в Kubernetes Docker Desktop. Новые журналы я сохраняю в `logs/`.

## 1. Состав проекта

```text
kafka_lab/
├── app/
│   ├── main.py
│   └── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── kafka-demo-deployment.yaml
├── k8s/kafka-deployment.yml
├── .gitlab-ci.yml
├── .dockerignore
├── .gitignore
├── scripts/verify-desktop.sh
├── Homework_30_Kafka.md
├── logs/
└── previous-kind-logs/
```

## 2. Запуск Kafka и приложения

Я выполняю команды из корня проекта. Я использую Docker Compose для запуска Kafka и ZooKeeper, затем явно создаю топик с одной партицией и одной репликой.

```bash
docker compose config --quiet
docker compose up -d --wait kafka
docker compose ps
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic test-topic --partitions 1 --replication-factor 1
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic test-topic
docker compose build app
docker compose run --rm --no-deps app
```

Я проверяю строку `Проверка: OK`: приложение отправляет сообщение с уникальным UUID и проверяет получение именно этого сообщения. В контейнере я использую адрес `kafka:9092`, при запуске на компьютере — `localhost:29092`.

Для запуска Python-приложения вне контейнера я выполняю:

```bash
python -m pip install -r app/requirements.txt
python app/main.py
```

## 3. Kubernetes Docker Desktop

Я открываю Docker Desktop → Settings → Kubernetes, включаю Kubernetes и жду готовности. В терминале я проверяю Docker-контекст и наличие контекста Kubernetes `docker-desktop`:

```bash
docker context ls
docker info --format '{{.OperatingSystem}}'
kubectl config get-contexts
kubectl --context docker-desktop get nodes
```

Я выполняю проверочный сценарий в Bash и сохраняю его вывод. Благодаря `pipefail` ошибка сценария не скрывается успешным выполнением `tee`:

```bash
set -o pipefail
bash scripts/verify-desktop.sh 2>&1 | tee logs/desktop-kubernetes.txt
```

Я использую явный `--context docker-desktop` во всех командах Kubernetes. Сценарий проверяет Docker Desktop, собирает образ, разворачивает ZooKeeper и Kafka, создаёт топик, применяет `kafka-demo-deployment.yaml` и проверяет доставку сообщения. Я не подменяю этот запуск проверкой в kind.

Я использую локальный образ `kafka-demo:local` с `imagePullPolicy: Never`. Если Kubernetes сообщает `ErrImageNeverPull`, я проверяю доступность образа в хранилище выбранного кластера и устраняю проблему до повторной проверки. Я не считаю один только статус Running подтверждением обмена.

## 4. Репозиторий и CI/CD

Я подготовил `.gitlab-ci.yml` со стадиями `build`, `test` и `deploy`. Я использую один Shell Runner с тегом `kafka-lab`, доступом к Docker и Kubernetes Docker Desktop. Локальный Docker daemon сохраняет образ между стадиями; я запускаю эти задания на одном Runner. Я выполняю deploy только для основной ветки. Я не включаю токены и kubeconfig в репозиторий.

Я создал публичный репозиторий проекта на GitHub: [ultrovap-cyber/kafka-lab](https://github.com/ultrovap-cyber/kafka-lab). Я сохраняю в нём исходники и конфигурацию GitLab CI/CD. Файл `.gitlab-ci.yml` предназначен для запуска в GitLab и не запускается автоматически в GitHub. По условию я подготавливаю файл пайплайна; серверный запуск отдельно не требуется.

Я разместил проект в основной ветке `main`. Для сдачи я использую [ссылку на Markdown-отчёт](https://github.com/ultrovap-cyber/kafka-lab/blob/main/Homework_30_Kafka.md).

## 5. Фактическое состояние

Я перенёс зависимости в `app/requirements.txt`, обновил Dockerfile и команды установки. Я переименовал манифест приложения в `kafka-demo-deployment.yaml` и обновил CI/CD. Я подготовил отдельный сценарий проверки Kubernetes Docker Desktop.

Я опубликовал исправленный проект в публичном репозитории [ultrovap-cyber/kafka-lab](https://github.com/ultrovap-cyber/kafka-lab). Я предоставляю [прямую ссылку на этот Markdown-отчёт](https://github.com/ultrovap-cyber/kafka-lab/blob/main/Homework_30_Kafka.md). Я пока не подтвердил запуск Kubernetes Docker Desktop: в доступной среде обнаружен Docker Engine, а контекст Docker Desktop отсутствует. Я завершу проверку Kubernetes Docker Desktop перед повторной сдачей и добавлю журнал успешного запуска.

### Повторная проверка исправленного проекта

Я собрал образ `kafka-demo:revised` с зависимостями из `app/requirements.txt`. Я проверил конфигурацию Compose, готовность Kafka, создание топика и обмен новым сообщением. Я получил `Проверка: OK` и код завершения 0. После проверки я удалил только временный стенд. Я сохранил полный вывод в [logs/revised-compose.txt](logs/revised-compose.txt). Я выполнил эту проверку через Docker Engine; запуск Docker Desktop остаётся отдельным незавершённым пунктом.

## 6. Исходные файлы

### `app/main.py`

Я отправляю сообщение и проверяю его получение; интервал задаю через DEMO_INTERVAL.

```python
import os
import time
from uuid import uuid4

from kafka import KafkaConsumer, KafkaProducer


def exchange():
    server = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")
    topic = os.getenv("TOPIC", "test-topic")
    message = f"Hello Kafka! id={uuid4()}".encode("utf-8")

    # Отправляю сообщение и дожидаюсь подтверждения брокера.
    producer = KafkaProducer(bootstrap_servers=server)
    result = producer.send(topic, message).get(timeout=30)
    producer.flush(timeout=30)
    producer.close()
    print(f"Отправлено: {message.decode()}", flush=True)
    print(f"partition={result.partition}, offset={result.offset}", flush=True)

    # Читаю топик и проверяю получение именно моего сообщения.
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=server,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
        consumer_timeout_ms=10000,
    )
    try:
        for record in consumer:
            if record.value == message:
                print(f"Получено: {record.value.decode()}", flush=True)
                print("Проверка: OK", flush=True)
                return
        raise RuntimeError("Отправленное сообщение не найдено в топике")
    finally:
        consumer.close()


if __name__ == "__main__":
    # В Kubernetes повторяю обмен, а в обычном запуске выполняю его один раз.
    interval = float(os.getenv("DEMO_INTERVAL", "0"))
    while True:
        exchange()
        if interval <= 0:
            break
        time.sleep(interval)
```

### `app/requirements.txt`

Я закрепил зависимость kafka-python; остальные импорты входят в стандартную библиотеку.

```text
kafka-python==2.2.15
```

### `Dockerfile`

Я копирую зависимости из app и устанавливаю их перед копированием кода.

```dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY app/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/
CMD ["python", "-u", "app/main.py"]
```

### `docker-compose.yml`

Я запускаю один брокер и ZooKeeper. Я проверяю готовность Kafka через healthcheck.

```yaml
name: kafka-lab
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.9.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
  kafka:
    image: confluentinc/cp-kafka:7.9.0
    depends_on:
      - zookeeper
    ports:
      - "127.0.0.1:${KAFKA_PORT:-29092}:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:29092
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://localhost:${KAFKA_PORT:-29092}
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_HEAP_OPTS: "-Xms256m -Xmx256m"
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
      interval: 10s
      timeout: 10s
      retries: 18
      start_period: 20s
  app:
    image: ${APP_IMAGE:-kafka-demo:local}
    build: .
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      TOPIC: test-topic
    depends_on:
      kafka:
        condition: service_healthy
```

### `k8s/kafka-deployment.yml`

Я создаю namespace kafka-lab, Services и Deployments Kafka и ZooKeeper.

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: kafka-lab
---
apiVersion: v1
kind: Service
metadata:
  name: zookeeper
  namespace: kafka-lab
spec:
  selector:
    app: zookeeper
  ports:
    - port: 2181
      targetPort: 2181
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: zookeeper
  namespace: kafka-lab
spec:
  replicas: 1
  selector:
    matchLabels:
      app: zookeeper
  template:
    metadata:
      labels:
        app: zookeeper
    spec:
      containers:
        - name: zookeeper
          image: confluentinc/cp-zookeeper:7.9.0
          env:
            - name: ZOOKEEPER_CLIENT_PORT
              value: "2181"
            - name: ZOOKEEPER_TICK_TIME
              value: "2000"
          readinessProbe:
            tcpSocket:
              port: 2181
            initialDelaySeconds: 10
            periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: kafka
  namespace: kafka-lab
spec:
  publishNotReadyAddresses: true
  selector:
    app: kafka
  ports:
    - port: 9092
      targetPort: 9092
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka
  namespace: kafka-lab
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      enableServiceLinks: false
      containers:
        - name: kafka
          image: confluentinc/cp-kafka:7.9.0
          env:
            - name: KAFKA_BROKER_ID
              value: "1"
            - name: KAFKA_ZOOKEEPER_CONNECT
              value: zookeeper:2181
            - name: KAFKA_LISTENERS
              value: PLAINTEXT://0.0.0.0:9092
            - name: KAFKA_ADVERTISED_LISTENERS
              value: PLAINTEXT://kafka:9092
            - name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
              value: "1"
            - name: KAFKA_AUTO_CREATE_TOPICS_ENABLE
              value: "false"
            - name: KAFKA_HEAP_OPTS
              value: "-Xms256m -Xmx256m"
          readinessProbe:
            exec:
              command: ["kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
            initialDelaySeconds: 20
            periodSeconds: 10
            timeoutSeconds: 10
            failureThreshold: 18
```

### `kafka-demo-deployment.yaml`

Я разворачиваю приложение с постоянным обменом сообщениями.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-demo
  namespace: kafka-lab
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-demo
  template:
    metadata:
      labels:
        app: kafka-demo
    spec:
      containers:
        - name: app
          image: kafka-demo:local
          imagePullPolicy: Never
          env:
            - name: BOOTSTRAP_SERVERS
              value: kafka:9092
            - name: TOPIC
              value: test-topic
            - name: DEMO_INTERVAL
              value: "30"
```

### `.gitlab-ci.yml`

Я собираю образ, проверяю обмен через Compose и разворачиваю приложение в Docker Desktop.

```yaml
stages: [build, test, deploy]

default:
  tags: [kafka-lab]

variables:
  APP_IMAGE: kafka-demo:$CI_COMMIT_SHA
  KUBE_CONTEXT: docker-desktop

build:
  stage: build
  script:
    - docker build -t "$APP_IMAGE" .

test:
  stage: test
  script:
    - export COMPOSE_PROJECT_NAME="kafka-ci-$CI_JOB_ID"
    - export KAFKA_PORT=0
    - trap 'docker compose down --volumes' EXIT
    - docker compose up -d --wait kafka
    - docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic test-topic --partitions 1 --replication-factor 1
    - docker compose run --rm --no-deps app

deploy:
  stage: deploy
  resource_group: kafka-lab
  rules:
    - if: '$CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH'
  script:
    - kubectl --context "$KUBE_CONTEXT" apply -f k8s/kafka-deployment.yml
    - kubectl --context "$KUBE_CONTEXT" -n kafka-lab rollout status deployment/zookeeper --timeout=180s
    - kubectl --context "$KUBE_CONTEXT" -n kafka-lab rollout status deployment/kafka --timeout=300s
    - kubectl --context "$KUBE_CONTEXT" -n kafka-lab exec deployment/kafka -- kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic test-topic --partitions 1 --replication-factor 1
    - sed "s|kafka-demo:local|$APP_IMAGE|g" kafka-demo-deployment.yaml | kubectl --context "$KUBE_CONTEXT" apply -f -
    - kubectl --context "$KUBE_CONTEXT" -n kafka-lab rollout status deployment/kafka-demo --timeout=180s
    - kubectl --context "$KUBE_CONTEXT" -n kafka-lab logs deployment/kafka-demo --tail=20
```

### `scripts/verify-desktop.sh`

Я проверяю кластер Docker Desktop и успешную доставку сообщения.

```bash
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
```

### `.dockerignore`

Я исключаю документацию и журналы из контекста сборки.

```text
.git
logs
*.md
__pycache__
k8s
.gitlab-ci.yml
previous-kind-logs
scripts
```

### `.gitignore`

Я исключаю временные файлы и kubeconfig.

```text
__pycache__/
*.pyc
.venv/
*.kubeconfig
```

## 7. Завершение работы

Я останавливаю учебные ресурсы только после сохранения результатов проверки:

```bash
docker compose down --volumes
kubectl --context docker-desktop delete namespace kafka-lab
```

Я учитываю, что эти команды удаляют ресурсы учебного стенда и сообщения Kafka.
