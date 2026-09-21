from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField


KAFKA_BOOTSTRAP_SERVERS = "kafka:9093"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
TOPIC = "orders"
GROUP_ID = "orders-consumer-group"


def main():
    schema_registry_client = SchemaRegistryClient(
        {
            "url": SCHEMA_REGISTRY_URL,
        }
    )

    avro_deserializer = AvroDeserializer(
        schema_registry_client,
    )

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe([TOPIC])

    print("Consumer started.", flush=True)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(
                    f"Consumer error: {msg.error()}",
                    flush=True,
                )
                continue

            event = avro_deserializer(
                msg.value(),
                SerializationContext(
                    TOPIC,
                    MessageField.VALUE,
                ),
            )

            print(
                f"Received: "
                f"orderId={event['orderId']}; "
                f"userId={event['userId']}; "
                f"partition={msg.partition()}; "
                f"offset={msg.offset()}",
                flush=True,
            )

    finally:
        consumer.close()


if __name__ == "__main__":
    main()