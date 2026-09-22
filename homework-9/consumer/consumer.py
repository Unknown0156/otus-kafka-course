import os

from quixstreams import Application


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

app = Application(
    broker_address=KAFKA_BOOTSTRAP_SERVERS,
    consumer_group=GROUP_ID,
    auto_offset_reset="earliest",
    processing_guarantee="exactly-once",
)


orders_topic = app.topic(
    "orders",
    value_deserializer="json",
)


sdf = app.dataframe(orders_topic)


def calculate_total(value, state):
    user_id = value["userId"]
    amount = value["amount"]

    total = state.get("total", 0)
    total += amount

    state.set("total", total)

    print(
        f"userId={user_id}; "
        f"total={total}",
        flush=True,
    )

    return {
        "userId": user_id,
        "total": total,
    }


sdf = sdf.group_by(
    lambda value: value["userId"],
    name="userId",
)

sdf = sdf.apply(
    calculate_total,
    stateful=True,
)


if __name__ == "__main__":
    app.run()