# Kafka Homework-2

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

### Retry

Retry нужен для повторной обработки сообщения, если произошла временная ошибка.

### Backoff

Backoff задаёт задержку между повторными попытками, чтобы не создавать нагрузку на систему при повторной обработке.

### DLT

Dead Letter Topic (DLT) используется для сообщений, которые не удалось обработать после заданного количества попыток.

### Работа с сообщениями из DLT

Сообщения из DLT необходимо анализировать, определить причину ошибки и после её устранения при необходимости повторно отправить сообщение на обработку.

## Результаты проверки

### Результаты проверки работы Consumer с retry и DLT

Consumerуспешно отправляет сообщения в retry и DLT топики.
В логах видно:
успешную обработку обычных сообщений;
номер попытки обработки ошибочного сообщения;
переход сообщения в retry;
попадание сообщения в DLT после исчерпания попыток.

- [Consumer retry and DLT](screenshots/consumer-retry-dlt.png) 