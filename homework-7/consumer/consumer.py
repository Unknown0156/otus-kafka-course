import json
import os
import signal

from kafka import KafkaConsumer
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from kafka.coordinator.assignors.sticky.sticky_assignor import StickyPartitionAssignor


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

GROUP_INSTANCE_ID = os.getenv(
    "GROUP_INSTANCE_ID",
    CONSUMER_NAME,
)


def deserialize_key(key):
    if key is None:
        return None

    return key.decode("utf-8")


def deserialize_value(value):
    return json.loads(value.decode("utf-8"))


class RebalanceListener(ConsumerRebalanceListener):

    def on_partitions_revoked(self, revoked):
        print(
            f"{CONSUMER_NAME}; "
            f"PARTITIONS REVOKED; "
            f"partitions={[partition.partition for partition in revoked]}",
            flush=True,
        )

    def on_partitions_assigned(self, assigned):
        print(
            f"{CONSUMER_NAME}; "
            f"PARTITIONS ASSIGNED; "
            f"partitions={[partition.partition for partition in assigned]}",
            flush=True,
        )


def process_message(message):
    print(
        f"{CONSUMER_NAME}; "
        f"PROCESSING; "
        f"key={message.key}; "
        f"partition={message.partition}; "
        f"offset={message.offset}; "
        f"message={message.value}",
        flush=True,
    )

    print(
        f"{CONSUMER_NAME}; "
        f"PROCESSED; "
        f"partition={message.partition}; "
        f"offset={message.offset}",
        flush=True,
    )


shutdown = False


def handle_shutdown(signum, frame):
    global shutdown

    print(
        f"{CONSUMER_NAME}; "
        f"SHUTDOWN SIGNAL; "
        f"signal={signum}",
        flush=True,
    )

    shutdown = True


consumer = KafkaConsumer(
    "orders",
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    group_instance_id=GROUP_INSTANCE_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    max_poll_records=1,
    key_deserializer=deserialize_key,
    value_deserializer=deserialize_value,
    partition_assignment_strategy=[StickyPartitionAssignor],
)


signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


print(
    f"{CONSUMER_NAME}; "
    f"STARTED; "
    f"group={GROUP_ID}; "
    f"instance_id={GROUP_INSTANCE_ID}",
    flush=True,
)


consumer.subscribe(
    ["orders"],
    listener=RebalanceListener(),
)


try:
    while not shutdown:
        records = consumer.poll(timeout_ms=1000)

        for messages in records.values():
            for message in messages:
                if shutdown:
                    break

                try:
                    process_message(message)

                    consumer.commit()

                    print(
                        f"{CONSUMER_NAME}; "
                        f"COMMITTED; "
                        f"partition={message.partition}; "
                        f"offset={message.offset}",
                        flush=True,
                    )

                except Exception as error:
                    print(
                        f"{CONSUMER_NAME}; "
                        f"ERROR; "
                        f"partition={message.partition}; "
                        f"offset={message.offset}; "
                        f"error={error}",
                        flush=True,
                    )

                    shutdown = True
                    break

finally:
    print(
        f"{CONSUMER_NAME}; "
        f"CLOSING",
        flush=True,
    )

    consumer.close()

    print(
        f"{CONSUMER_NAME}; "
        f"CLOSED",
        flush=True,
    )