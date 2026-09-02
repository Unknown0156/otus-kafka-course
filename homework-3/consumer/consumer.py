import json
import os
import time

from kafka import KafkaConsumer, KafkaProducer


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

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda key: str(key).encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

consumer = KafkaConsumer(
    "orders",
    "orders-retry",
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    max_poll_records=1,
    key_deserializer=deserialize_key,
    value_deserializer=deserialize_value,
)


for message in consumer:
    order = message.value

    if message.topic == "orders":
        attempt = 1
    else:
        attempt = order.get("attempt", 2)
        time.sleep(2)

    print(
        f"PROCESSING; "
        f"orderId={order['orderId']}; "
        f"attempt={attempt}; "
        f"topic={message.topic}",
        flush=True,
    )

    try:
        # Искусственная ошибка для orderId=5
        if order["orderId"] == 5:
            raise RuntimeError("Artificial processing error")

        print(
            f"SUCCESS; "
            f"orderId={order['orderId']}; "
            f"attempt={attempt}",
            flush=True,
        )

        consumer.commit()

    except Exception as error:
        print(
            f"ERROR; "
            f"orderId={order['orderId']}; "
            f"attempt={attempt}; "
            f"error={error}",
            flush=True,
        )

        if attempt < 3:
            next_attempt = attempt + 1

            producer.send(
                "orders-retry",
                key=order["userId"],
                value={
                    **order,
                    "attempt": next_attempt,
                },
            ).get(timeout=10)

            print(
                f"RETRY; "
                f"orderId={order['orderId']}; "
                f"next_attempt={next_attempt}",
                flush=True,
            )

        else:
            producer.send(
                "orders-dlt",
                key=order["userId"],
                value=order,
            ).get(timeout=10)

            print(
                f"DLT; "
                f"orderId={order['orderId']}; "
                f"attempt={attempt}; "
                f"topic=orders-dlt",
                flush=True,
            )

        # Исходное сообщение уже либо ушло в retry,
        # либо в DLT — поэтому можем зафиксировать offset.
        consumer.commit()