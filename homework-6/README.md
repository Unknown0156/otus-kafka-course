# Kafka Homework-6

Проект выполнен в рамках заданий по Apache Kafka.

## Структура проекта

```text
homework-6/
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
cd homework-6
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

### Что хранит Schema Registry

Schema Registry хранит схемы сообщений и их версии, используемые для сериализации и десериализации данных. Также он хранит настройки совместимости схем и позволяет проверять новые версии перед их использованием.

### Что означает BACKWARD compatibility

BACKWARD означает, что новая версия схемы должна быть совместима со старыми данными, созданными предыдущей версией схемы. Это позволяет новому Consumer читать сообщения, записанные со старой схемой.

### Почему первая модификация совместима, а вторая — нет

Первая модификация добавляет поле createdAt с значением default, поэтому старая запись без этого поля может быть прочитана новой схемой.

Вторая модификация меняет тип существующего поля userId с int на string. Из-за изменения типа новая схема не может корректно интерпретировать старые сообщения, поэтому проверка BACKWARD возвращает False.

- [Schema compatible](screenshots/schema-compatible.png)
- [Schema incompatible](screenshots/schema-incompatible.png) 