import json
import os

from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

CONSUMER_NAME = os.getenv(
    "CONSUMER_NAME",
    "consumer",
)

GROUP_ID = os.getenv(
    "GROUP_ID",
    "orders-consumer-group",
)

def deserialize_key(key):
    if key is None:
        return None

    return key.decode("utf-8")


def deserialize_value(value):
    return json.loads(value.decode("utf-8"))

def process_message(message):
    print(
        f"{CONSUMER_NAME}; "
        f"PROCESSING; "
        f"key={message.key}; "
        f"partition={message.partition}; "
        f"offset={message.offset}; "
        f"message={message.value}",
        flush=True,
    )

    # Здесь находится обработка сообщения.
    # Если обработка завершилась успешно — функция заканчивается нормально.

    print(
        f"{CONSUMER_NAME}; "
        f"PROCESSED; "
        f"partition={message.partition}; "
        f"offset={message.offset}",
        flush=True,
    )

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    max_poll_records=1,
    key_deserializer=deserialize_key,
    value_deserializer=deserialize_value,
)


for message in consumer:
    try:
        process_message(message)

        consumer.commit()

        print(
            f"{CONSUMER_NAME}; "
            f"COMMITTED; "
            f"partition={message.partition}; "
            f"offset={message.offset}",
            flush=True,
        )

    except Exception as error:
        print(
            f"{CONSUMER_NAME}; "
            f"ERROR; "
            f"partition={message.partition}; "
            f"offset={message.offset}; "
            f"error={error}",
            flush=True,
        )

        break