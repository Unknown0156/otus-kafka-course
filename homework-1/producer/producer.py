import json
import os

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda key: str(key).encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

orders = [
    {"orderId": 1, "userId": 10, "product": "Keyboard"},
    {"orderId": 2, "userId": 20, "product": "Mouse"},
    {"orderId": 3, "userId": 10, "product": "Monitor"},
    {"orderId": 4, "userId": 30, "product": "Headphones"},
    {"orderId": 5, "userId": 20, "product": "Webcam"},
    {"orderId": 6, "userId": 10, "product": "Microphone"},
    {"orderId": 7, "userId": 40, "product": "Laptop"},
    {"orderId": 8, "userId": 30, "product": "USB Hub"},
    {"orderId": 9, "userId": 20, "product": "SSD"},
    {"orderId": 10, "userId": 40, "product": "Keyboard"},
]


for order in orders:
    future = producer.send(
        "orders",
        key=order["userId"],
        value=order,
    )

    metadata = future.get(timeout=10)

    print(
        f"userId: {order['userId']}; "
        f"partition: {metadata.partition}; "
        f"offset: {metadata.offset}"
    )


producer.flush()
producer.close()