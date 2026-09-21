import json
import os

import psycopg
from kafka import KafkaProducer


DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "orders")
DB_USER = os.getenv("DB_USER", "orders")
DB_PASSWORD = os.getenv("DB_PASSWORD", "orders")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)


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
                CREATE TABLE IF NOT EXISTS orders (
                    order_id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    product VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS outbox (
                    id SERIAL PRIMARY KEY,
                    event_id VARCHAR(255) NOT NULL UNIQUE,
                    event_type VARCHAR(255) NOT NULL,
                    payload JSONB NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    processed BOOLEAN NOT NULL DEFAULT FALSE
                )
            """)

        conn.commit()

    print("Database initialized", flush=True)


def create_order(order):
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Сохраняем заказ
            cur.execute(
                """
                INSERT INTO orders (
                    order_id,
                    user_id,
                    product
                )
                VALUES (%s, %s, %s)
                """,
                (
                    order["orderId"],
                    order["userId"],
                    order["product"],
                ),
            )

            # И событие в Outbox
            cur.execute(
                """
                INSERT INTO outbox (
                    event_id,
                    event_type,
                    payload
                )
                VALUES (%s, %s, %s)
                """,
                (
                    order["eventId"],
                    "OrderCreated",
                    json.dumps(order),
                ),
            )

        # Заказ и Outbox сохраняются атомарно.
        conn.commit()

    print(
        f"ORDER CREATED; "
        f"orderId={order['orderId']}; "
        f"eventId={order['eventId']}",
        flush=True,
    )

    print(
        f"OUTBOX SAVED; "
        f"eventId={order['eventId']}",
        flush=True,
    )


def get_unprocessed_event(event_id):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, event_type, payload
                FROM outbox
                WHERE event_id = %s
                  AND processed = FALSE
                """,
                (event_id,),
            )

            return cur.fetchone()


def mark_processed(outbox_id):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE outbox
                SET processed = TRUE
                WHERE id = %s
                """,
                (outbox_id,),
            )

        conn.commit()


def send_event(producer, event_id, simulate_failure=False):
    event = get_unprocessed_event(event_id)

    if event is None:
        print(
            f"OUTBOX EVENT NOT FOUND; eventId={event_id}",
            flush=True,
        )
        return

    outbox_id, event_type, payload = event

    print(
        f"OUTBOX; "
        f"id={outbox_id}; "
        f"eventId={event_id}; "
        f"eventType={event_type}",
        flush=True,
    )

    first_attempt = True

    while True:
        try:
            print(
                f"KAFKA SEND ATTEMPT; "
                f"eventId={event_id}",
                flush=True,
            )

            producer.send(
                "orders",
                key=event_id,
                value=payload,
            ).get(timeout=10)

            # Искусственно ломаем только первую попытку, вторая и последующие попытки уже настоящие
            if simulate_failure and first_attempt:
                first_attempt = False

                raise RuntimeError(
                    "Simulated Kafka failure"
                )

            # Сюда попадём только если Kafka действительно подтвердила успешную отправку
            print(
                f"KAFKA SENT; "
                f"eventId={event_id}; "
                f"topic=orders",
                flush=True,
            )

            mark_processed(outbox_id)

            print(
                f"OUTBOX PROCESSED; "
                f"eventId={event_id}",
                flush=True,
            )

            break

        except Exception as error:
            print(
                f"KAFKA SEND FAILED; "
                f"eventId={event_id}; "
                f"error={type(error).__name__}: {error}",
                flush=True,
            )

            print(
                f"KAFKA RETRY; "
                f"eventId={event_id}",
                flush=True,
            )


def main():
    init_db()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    orders = [
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
    ]

    try:
        # Заказ 1: обычная отправка
        create_order(orders[0])
        send_event(producer, "event-1")

        # Заказ 2: первая попытка падает, затем сразу происходит retry
        create_order(orders[1])
        send_event(
            producer,
            "event-2",
            simulate_failure=True,
        )

        # Заказ 3: обычная отправка после восстановления
        create_order(orders[2])
        send_event(producer, "event-3")

        producer.flush()

    finally:
        producer.close()


if __name__ == "__main__":
    main()