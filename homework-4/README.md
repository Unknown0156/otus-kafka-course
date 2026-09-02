# Kafka Homework-1

Проект выполнен в рамках заданий по Apache Kafka.

## Структура проекта

```text
homework-1/
├── docker-compose.yml
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── consumer.py
└── README.md
```

## Запуск проекта

Для запуска проекта необходимо установить Docker и Docker Compose.

Перейти в корневую директорию проекта:

```bash
cd homework-1
```

Запустить Kafka, создать topic `orders`, а также собрать и запустить producer и consumer:

```bash
docker compose up --build
```

При запуске Kafka автоматически создаётся topic `orders` с тремя partitions.

Проверить созданный topic можно командой:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic orders
```

Остановить проект:

```bash
docker compose down
```

## Документация

### Почему Kafka может доставить сообщение повторно?

Если Consumer обработал сообщение, но не успел подтвердить offset, Kafka считает сообщение необработанным и доставляет его повторно.

### Как Inbox делает Consumer идемпотентным?

Consumer сохраняет `eventId` обработанного сообщения в таблицу `inbox`. При повторной доставке он проверяет `eventId` и, если он уже есть в `inbox`, пропускает бизнес-операцию.

## Проверка дубля и бизнес-операция должны

Результат проверки зафиксирован на скриншоте логов:
- [Consumers с одинаковым group.id](screenshots/consumer-inbox-duplicate.png)