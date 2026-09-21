# Kafka Homework-7

Проект выполнен в рамках заданий по Apache Kafka.

## Структура проекта

```text
homework-7/
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
cd homework-7
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

### Когда возникает rebalance

Rebalance возникает, когда изменяется состав consumer group или необходимо изменить распределение partitions между consumer'ами.
- [Rebalancing](screenshots/rebalancing.png)

### Для чего нужен cooperative assignor

Cooperative assignor позволяет выполнять incremental rebalance: перераспределяются только необходимые partitions, без полного отзыва всех partitions у consumer'ов.

В используемом `kafka-python 3.0.11` отдельный `CooperativeStickyAssignor` отсутствует. Доступен `StickyPartitionAssignor`, но он не является полноценной заменой `CooperativeStickyAssignor`. Поэтому требуемый эксперимент с cooperative assignor в точности выполнить на используемом Python-клиенте не удалось.

### Какую проблему решает static membership

Static membership позволяет закрепить за consumer постоянный `group.instance.id`.
При кратковременном перезапуске consumer Kafka может сохранить его членство в consumer group и не считать его новым участником. Это уменьшает количество лишних rebalance при временных перезапусках.
- [Static membership](screenshots/static-membership.png)

### Зачем нужен graceful shutdown

Graceful shutdown позволяет consumer корректно завершить обработку и вызвать `consumer.close()` перед завершением приложения.
При получении `SIGTERM` или `SIGINT` consumer выходит из цикла обработки, закрывается и корректно покидает consumer group. Это позволяет избежать некорректного завершения и лишних задержек при обнаружении отключившегося consumer.
- [Graceful shutdown](screenshots/graceful-shutdown.png)