# Kafka: учебный проект

Я реализовал отправку и чтение сообщений Kafka, запуск через Docker Compose, сборку образа, конфигурацию GitLab CI/CD и манифесты Kubernetes.

Я собрал подробное решение в [Homework_30_Kafka.md](Homework_30_Kafka.md). Я сохранил результат повторной проверки сборки и доставки сообщения в [logs/revised-compose.txt](logs/revised-compose.txt).

Я разместил зависимости в `app/requirements.txt`, а манифест приложения — в `kafka-demo-deployment.yaml`. Я храню `.gitlab-ci.yml` как конфигурацию GitLab; на GitHub этот файл автоматически не выполняется.

Я подтвердил развёртывание и обмен сообщениями в Kubernetes Docker Desktop. Я сохранил вывод в [logs/desktop-kubernetes.txt](logs/desktop-kubernetes.txt). Для повторения я использую `scripts/verify-desktop.sh`. Журналы прежнего отдельного запуска в kind я сохранил в `previous-kind-logs/`.
