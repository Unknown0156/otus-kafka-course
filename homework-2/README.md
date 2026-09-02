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

### Что даёт `acks=all`?

Producer ждёт подтверждения записи сообщения от всех синхронных реплик. Это повышает надёжность доставки сообщения.

### Зачем нужны `retries`?

Позволяют повторить отправку сообщения при временной ошибке, например при недоступности broker.

### Какую проблему решает `enable.idempotence`?

Предотвращает появление дубликатов сообщений при повторной отправке одного сообщения.

### Почему commit offset выполняется после обработки сообщения?

Чтобы не потерять сообщение. Если сначала подтвердить offset, а затем обработка завершится ошибкой, Kafka будет считать сообщение обработанным и повторно его не отправит.

Правильный порядок:

```text
Получение сообщения
        ↓
Обработка
        ↓
Успешная обработка
        ↓
Commit offset
```

## Результаты проверки

### 1. Результаты проверки работы Producer и Consumer

Producer успешно отправляет сообщения в Kafka с подтверждением доставки, а Consumer успешно получает и обрабатывает отправленные сообщения.

- [Producer acknowledgement](screenshots/producer-acknowledgement.png)
- [Consumer manual commit](screenshots/consumer-manual-commit.png) 

### 2. Подтверждение корректной фиксации offset после обработки сообщений

Offset фиксируется только после успешной обработки сообщения. В логах видна последовательность:

```text
PROCESSING → PROCESSED → COMMITTED
```