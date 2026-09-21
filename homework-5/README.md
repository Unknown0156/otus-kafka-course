# Kafka Homework-5

Проект выполнен в рамках заданий по Apache Kafka.

## Структура проекта

```text
homework-5/
├── docker-compose.yml
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py
└── README.md
```

## Запуск проекта

Для запуска проекта необходимо установить Docker и Docker Compose.

Перейти в корневую директорию проекта:

```bash
cd homework-5
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

### Какую проблему решает Transactional Outbox?

Transactional Outbox гарантирует, что изменение в БД и событие для Kafka сохраняются атомарно. Если Kafka временно недоступна, событие не теряется и остаётся в Outbox для повторной отправки.

### Почему недостаточно последовательно выполнить save() и producer.send()?

Потому что между операциями может произойти сбой: запись в БД уже сохранится, а отправка в Kafka не выполнится. В результате состояние БД изменено, но соответствующее событие потеряно.

## Проверка Transactional Outbox failure:

Результат проверки зафиксирован на скриншоте логов:
- [Transactional Outbox failure](screenshots/transactional-outbox-failure.png)