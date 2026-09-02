import json
import os
import psycopg

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

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "orders")
DB_USER = os.getenv("DB_USER", "orders")
DB_PASSWORD = os.getenv("DB_PASSWORD", "orders")


def deserialize_key(key):
    if key is None:
        return None

    return key.decode("utf-8")


def deserialize_value(value):
    return json.loads(value.decode("utf-8"))


def get_connection():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def init_db():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS inbox (
                    event_id VARCHAR(255) PRIMARY KEY,
                    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    product VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

        conn.commit()


def process_message(message):
    event = message.value

    event_id = event["eventId"]
    order_id = event["orderId"]
    user_id = event["userId"]
    product = event["product"]

    with get_connection() as conn:
        with conn.cursor() as cur:

            # Проверяем, обрабатывали ли уже это событие
            cur.execute(
                """
                SELECT 1
                FROM inbox
                WHERE event_id = %s
                """,
                (event_id,),
            )

            if cur.fetchone():
                print(
                    f"DUPLICATE; "
                    f"eventId={event_id}; "
                    f"action=SKIPPED",
                    flush=True,
                )

                return False

            # Бизнес-операция
            cur.execute(
                """
                INSERT INTO orders (
                    order_id,
                    user_id,
                    product
                )
                VALUES (%s, %s, %s)
                """,
                (order_id, user_id, product),
            )

            print(
                f"BUSINESS OPERATION; "
                f"eventId={event_id}; "
                f"orderId={order_id}",
                flush=True,
            )

            # Фиксируем событие как обработанное
            cur.execute(
                """
                INSERT INTO inbox (event_id)
                VALUES (%s)
                """,
                (event_id,),
            )

            print(
                f"INBOX; "
                f"eventId={event_id}; "
                f"action=RECORDED",
                flush=True,
            )

        # COMMIT всей транзакции
        conn.commit()

    return True


consumer = KafkaConsumer(
    "orders",
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    key_deserializer=deserialize_key,
    value_deserializer=deserialize_value,
)

init_db()

for message in consumer:
    try:
        processed = process_message(message)

        consumer.commit()

        if processed:
            print(
                f"COMMITTED; "
                f"eventId={message.value['eventId']}",
                flush=True,
            )
        else:
            print(
                f"COMMITTED; "
                f"eventId={message.value['eventId']}; "
                f"duplicate=True",
                flush=True,
            )

    except Exception as error:
        print(
            f"ERROR; "
            f"eventId={message.value['eventId']}; "
            f"error={error}",
            flush=True,
        )