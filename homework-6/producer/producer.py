from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import (
    Schema,
    SchemaRegistryClient,
)
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
)


KAFKA_BOOTSTRAP_SERVERS = "kafka:9093"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

TOPIC = "orders"
SUBJECT = "orders-value"

SCHEMAS_PATH = Path("/app/schemas")


def read_schema(filename):
    path = SCHEMAS_PATH / filename
    return path.read_text()


def delivery_report(err, msg):
    if err is not None:
        print(
            f"Delivery failed: {err}",
            flush=True,
        )
        return

    print(
        f"Sent: orderId={msg.key().decode()}; "
        f"partition={msg.partition()}; "
        f"offset={msg.offset()}",
        flush=True,
    )


def main():
    schema_registry = SchemaRegistryClient(
        {"url": SCHEMA_REGISTRY_URL}
    )

    # Регистрируем первую версию схемы

    print("=== Register schema v1 ===", flush=True)

    schema_v1_str = read_schema(
        "order_created.avsc"
    )

    schema_v1 = Schema(
        schema_str=schema_v1_str,
        schema_type="AVRO",
    )

    v1_id = schema_registry.register_schema(
        SUBJECT,
        schema_v1,
    )

    print(
        f"Schema v1 registered: id={v1_id}",
        flush=True,
    )

    # Проверяем совместимость второй версии

    print(
        "=== Check schema v2 compatibility ===",
        flush=True,
    )

    schema_v2_str = read_schema(
        "order_created_v2.avsc"
    )

    schema_v2 = Schema(
        schema_str=schema_v2_str,
        schema_type="AVRO",
    )

    v2_compatible = schema_registry.test_compatibility(
        SUBJECT,
        schema_v2,
    )

    print(
        f"Schema v2 compatibility: {v2_compatible}",
        flush=True,
    )

    if not v2_compatible:
        raise RuntimeError(
            "Schema v2 is incompatible with schema v1"
        )

    print(
        "Schema v2 is compatible with schema v1.",
        flush=True,
    )

    # Регистрируем вторую версию

    print(
        "=== Register schema v2 ===",
        flush=True,
    )

    v2_id = schema_registry.register_schema(
        SUBJECT,
        schema_v2,
    )

    print(
        f"Schema v2 registered: id={v2_id}",
        flush=True,
    )

    # Проверяем несовместимую схему

    print(
        "=== Check incompatible schema ===",
        flush=True,
    )

    incompatible_schema_str = read_schema(
        "order_created_incompatible.avsc"
    )

    incompatible_schema = Schema(
        schema_str=incompatible_schema_str,
        schema_type="AVRO",
    )

    incompatible = schema_registry.test_compatibility(
        SUBJECT,
        incompatible_schema,
    )

    print(
        f"Incompatible schema compatibility: {incompatible}",
        flush=True,
    )

    if incompatible:
        raise RuntimeError(
            "Incompatible schema was unexpectedly accepted"
        )

    print(
        "Incompatible schema was correctly rejected.",
        flush=True,
    )

    # Продюсим сообщения с v2

    print(
        "=== Start producing events with schema v2 ===",
        flush=True,
    )

    avro_serializer = AvroSerializer(
        schema_registry,
        schema_v2_str,
    )

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        }
    )

    events = [
        {
            "orderId": 1001,
            "userId": 101,
            "createdAt": "2026-09-21T12:00:00Z",
        },
        {
            "orderId": 1002,
            "userId": 102,
            "createdAt": "2026-09-21T12:01:00Z",
        },
        {
            "orderId": 1003,
            "userId": 103,
            "createdAt": "2026-09-21T12:02:00Z",
        },
    ]

    for event in events:
        producer.produce(
            topic=TOPIC,
            key=str(event["orderId"]).encode(),
            value=avro_serializer(
                event,
                SerializationContext(
                    TOPIC,
                    MessageField.VALUE,
                ),
            ),
            on_delivery=delivery_report,
        )

        producer.poll(0)

    producer.flush()

    print(
        "All events sent.",
        flush=True,
    )


if __name__ == "__main__":
    main()