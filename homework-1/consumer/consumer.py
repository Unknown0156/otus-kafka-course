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


consumer = KafkaConsumer(
    "orders",
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    key_deserializer=deserialize_key,
    value_deserializer=deserialize_value,
)


for message in consumer:
    print(
        f"{CONSUMER_NAME}; "
        f"{message.key}; "
        f"{message.partition}; "
        f"{message.offset}; "
        f"{message.value}",
        flush=True,
    )