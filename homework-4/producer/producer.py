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

events = [
    {
        "eventId": "event-1",
        "orderId": 1,
        "userId": 10,
        "product": "Keyboard",
    },
    {
        "eventId": "event-2",
        "orderId": 2,
        "userId": 20,
        "product": "Mouse",
    },
    {
        "eventId": "event-3",
        "orderId": 3,
        "userId": 10,
        "product": "Monitor",
    },
    {
        "eventId": "event-4",
        "orderId": 4,
        "userId": 30,
        "product": "Headphones",
    },
    {
        "eventId": "event-5",
        "orderId": 5,
        "userId": 20,
        "product": "Webcam",
    },
]


for event in events:
    producer.send(
        "orders",
        key=event["userId"],
        value=event,
    ).get(timeout=10)

    print(
        f"SENT; "
        f"eventId={event['eventId']}; "
        f"orderId={event['orderId']}",
        flush=True,
    )


# Намеренно отправляем event-3 ещё два раза
duplicate_event = events[2]

for _ in range(2):
    producer.send(
        "orders",
        key=duplicate_event["userId"],
        value=duplicate_event,
    ).get(timeout=10)

    print(
        f"DUPLICATE SENT; "
        f"eventId={duplicate_event['eventId']}; "
        f"orderId={duplicate_event['orderId']}",
        flush=True,
    )


producer.flush()
producer.close()