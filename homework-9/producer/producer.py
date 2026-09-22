import json
import os
import uuid

from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    acks="all",
    retries=5,
    enable_idempotence=True,
    key_serializer=lambda key: str(key).encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

orders = [
    {"userId": 10, "product": "Keyboard", "amount": 5000},
    {"userId": 20, "product": "Mouse", "amount": 2000},
    {"userId": 10, "product": "Monitor", "amount": 30000},
    {"userId": 30, "product": "Headphones", "amount": 7000},
    {"userId": 20, "product": "Webcam", "amount": 6000},
    {"userId": 10, "product": "Microphone", "amount": 8000},
    {"userId": 40, "product": "Laptop", "amount": 100000},
    {"userId": 30, "product": "USB Hub", "amount": 3000},
    {"userId": 20, "product": "SSD", "amount": 12000},
    {"userId": 40, "product": "Keyboard", "amount": 5000},
]


for order_id, order_data in enumerate(orders, start=1):
    order = {
        "orderId": order_id,
        "userId": order_data["userId"],
        "product": order_data["product"],
        "amount": order_data["amount"],
    }

    correlation_id = str(uuid.uuid4())

    try:
        future = producer.send(
            "orders",
            key=order["orderId"],
            value=order,
            headers=[("correlationId", correlation_id.encode("utf-8"))],
        )

        metadata = future.get(timeout=10)

        print(
            f"SUCCESS; "
            f"orderId={order['orderId']}; "
            f"userId={order['userId']}; "
            f"amount={order['amount']}; "
            f"correlationId={correlation_id}; "
            f"partition={metadata.partition}; "
            f"offset={metadata.offset}",
            flush=True,
        )

    except KafkaError as error:
        print(
            f"ERROR; "
            f"orderId={order['orderId']}; "
            f"correlationId={correlation_id}; "
            f"error={error}",
            flush=True,
        )


producer.flush()
producer.close()