import json
import os
import uuid

from kafka import KafkaProducer
from kafka.errors import KafkaError


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

MESSAGE_COUNT = 1000


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    acks="all",
    retries=5,
    enable_idempotence=True,
    key_serializer=lambda key: str(key).encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


orders = [
    {"userId": 10, "product": "Keyboard"},
    {"userId": 20, "product": "Mouse"},
    {"userId": 10, "product": "Monitor"},
    {"userId": 30, "product": "Headphones"},
    {"userId": 20, "product": "Webcam"},
    {"userId": 10, "product": "Microphone"},
    {"userId": 40, "product": "Laptop"},
    {"userId": 30, "product": "USB Hub"},
    {"userId": 20, "product": "SSD"},
    {"userId": 40, "product": "Keyboard"},
]


for i in range(MESSAGE_COUNT):
    template = orders[i % len(orders)]

    order = {
        "orderId": i + 1,
        "userId": template["userId"],
        "product": template["product"],
    }

    correlation_id = str(uuid.uuid4())

    try:
        future = producer.send(
            "orders",
            key=order["userId"],
            value=order,
            headers=[
                ("correlationId", correlation_id.encode("utf-8")),
            ],
        )

        metadata = future.get(timeout=10)

        print(
            f"SUCCESS; "
            f"orderId={order['orderId']}; "
            f"correlationId={correlation_id}; "
            f"partition={metadata.partition}; "
            f"offset={metadata.offset}"
        )

    except KafkaError as error:
        print(
            f"ERROR; "
            f"orderId={order['orderId']}; "
            f"correlationId={correlation_id}; "
            f"error={error}"
        )


producer.flush()
producer.close()

print(f"Finished; sent={MESSAGE_COUNT}")