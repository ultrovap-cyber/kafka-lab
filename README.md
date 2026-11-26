# Kafka: учебный проект

Я реализовал отправку и чтение сообщений Kafka, запуск через Docker Compose, сборку образа, конфигурацию GitLab CI/CD и манифесты Kubernetes.

Я собрал подробное решение в [Homework_30_Kafka.md](Homework_30_Kafka.md). Я сохранил результат повторной проверки сборки и доставки сообщения в [logs/revised-compose.txt](logs/revised-compose.txt).

Я разместил зависимости в `app/requirements.txt`, а манифест приложения — в `kafka-demo-deployment.yaml`. Я храню `.gitlab-ci.yml` как конфигурацию GitLab; на GitHub этот файл автоматически не выполняется.

Я пока не подтвердил проверку в Kubernetes Docker Desktop. Для её выполнения я подготовил `scripts/verify-desktop.sh`. Журналы прежнего запуска в kind я сохранил отдельно в `previous-kind-logs/`.
