import json
import os
import time

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


processed_messages = 0
errors = 0


def deserialize_key(key):
    if key is None:
        return None

    return key.decode("utf-8")


def deserialize_value(value):
    return json.loads(value.decode("utf-8"))


def get_correlation_id(message):
    for key, value in message.headers:
        if key == "correlationId":
            return value.decode("utf-8")

    return "N/A"


def get_lag():
    assigned_partitions = consumer.assignment()

    if not assigned_partitions:
        return 0

    end_offsets = consumer.end_offsets(assigned_partitions)

    lag = 0

    for partition in assigned_partitions:
        current_offset = consumer.position(partition)
        end_offset = end_offsets[partition]

        lag += max(0, end_offset - current_offset)

    return lag


def print_metrics():
    lag = get_lag()

    print(
        f"{CONSUMER_NAME}; "
        f"METRICS; "
        f"processed_messages={processed_messages}; "
        f"errors={errors}; "
        f"lag={lag}",
        flush=True,
    )


def process_message(message):
    correlation_id = get_correlation_id(message)

    print(
        f"{CONSUMER_NAME}; "
        f"PROCESSING; "
        f"correlationId={correlation_id}; "
        f"key={message.key}; "
        f"partition={message.partition}; "
        f"offset={message.offset}; "
        f"message={message.value}",
        flush=True,
    )

    # Искусственно замедляем обработку
    # для демонстрации роста consumer lag.
    time.sleep(1)

    print(
        f"{CONSUMER_NAME}; "
        f"PROCESSED; "
        f"correlationId={correlation_id}; "
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
    correlation_id = get_correlation_id(message)

    try:
        process_message(message)

        consumer.commit()

        processed_messages += 1

        print(
            f"{CONSUMER_NAME}; "
            f"COMMITTED; "
            f"correlationId={correlation_id}; "
            f"partition={message.partition}; "
            f"offset={message.offset}",
            flush=True,
        )

        print_metrics()

    except Exception as error:
        errors += 1

        print(
            f"{CONSUMER_NAME}; "
            f"ERROR; "
            f"correlationId={correlation_id}; "
            f"partition={message.partition}; "
            f"offset={message.offset}; "
            f"error={error}",
            flush=True,
        )

        print_metrics()

        break