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
