# Kafka Homework-8

Проект выполнен в рамках заданий по Apache Kafka.

## Структура проекта

```text
homework-8/
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
cd homework-8
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

### Что такое consumer lag

Consumer lag — это количество сообщений, которые уже записаны в Kafka, но ещё не обработаны consumer'ом. Чем больше lag, тем сильнее consumer отстаёт от producer'а.
- [Consumer lag](screenshots/consumer-lag.png)

### Зачем нужен correlation ID

Correlation ID — уникальный идентификатор сообщения, который позволяет проследить его прохождение через систему. В нашем случае по нему можно найти конкретное сообщение в логах producer и consumer и связать его отправку с последующей обработкой.
- [Correlation ID](screenshots/correlation-id.png)