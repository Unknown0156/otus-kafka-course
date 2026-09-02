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

## Producer

Producer отправляет сообщения в topic `orders`.

В качестве ключа сообщения используется `userId`.

Для каждого отправленного сообщения выводятся:

* `userId`
* `partition`
* `offset`

Пример:

```text
producer-1    | userId: 10; partition: 1; offset: 0
producer-1    | userId: 20; partition: 1; offset: 1
producer-1    | userId: 10; partition: 1; offset: 2
producer-1    | userId: 30; partition: 0; offset: 0
producer-1    | userId: 20; partition: 1; offset: 3
producer-1    | userId: 10; partition: 1; offset: 4
producer-1    | userId: 40; partition: 2; offset: 0
producer-1    | userId: 30; partition: 0; offset: 1
producer-1    | userId: 20; partition: 1; offset: 5
producer-1    | userId: 40; partition: 2; offset: 1
```

## Consumer

Consumer читает сообщения из topic `orders`.

Для каждого полученного сообщения выводятся:

* имя consumer;
* key;
* partition;
* offset;
* содержимое сообщения.

Consumer использует переменные окружения для настройки имени и `group.id`.

## Проверка Consumer Groups

Для проверки consumer groups были запущены два consumer с одинаковым `group.id`.

В результате partitions были распределены между consumer внутри одной группы.

После этого был запущен consumer с другим `group.id`.

Consumer новой группы читал сообщения независимо от первой consumer group.

Результат проверки зафиксирован на скриншоте логов:
- [Consumers с одинаковым group.id](screenshots/same-group-id.png)
- [Consumer с другим group.id](screenshots/another-group-id.png)

## Проверка Partition Distribution

Для проверки partitioning были отправлены сообщения с повторяющимися значениями `userId`.

По результатам проверки сообщения с одинаковым `userId` попадали в одну и ту же partition.

Например:

```text
userId: 30 → partition: 0
userId: 30 → partition: 0

userId: 20 → partition: 1
userId: 20 → partition: 1
userId: 20 → partition: 1
```

Это подтверждает корректную работу распределения сообщений по ключу.

Результат проверки зафиксирован на скриншоте логов:
- [Partition Distribution](screenshots/partition-distribution.png)