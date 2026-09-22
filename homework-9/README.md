# Kafka Homework-9

Проект выполнен в рамках заданий по Apache Kafka.

## Структура проекта

```text
homework-9/
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
cd homework-9
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

### Для чего нужен state store?

State store хранит текущее состояние потоковой обработки. В нашем случае он хранит накопленную сумму заказов для каждого `userId`.

### Что происходит с состоянием после перезапуска приложения?

После перезапуска состояние восстанавливается, поэтому подсчёт продолжается с предыдущего значения, а не начинается заново.

### Откуда Kafka Streams восстанавливает состояние после потери локального состояния?

Kafka Streams восстанавливает состояние из внутреннего changelog topic, который хранится в Kafka.

### Что даёт exactly_once_v2?

`exactly_once_v2` обеспечивает обработку сообщений ровно один раз с помощью транзакций, предотвращая повторную фиксацию результатов при сбоях.

### Почему при изменении ключа может потребоваться repartition topic?

При изменении ключа сообщения необходимо перераспределить между partitions в соответствии с новым ключом. Для этого Kafka Streams может использовать внутренний repartition topic.

### Какие internal topics были созданы?

При работе приложения были созданы внутренние Kafka Streams topics: changelog__orders-consumer-group--repartition.orders.userId--default и repartition topic - repartition__orders-consumer-group--orders--userId, используемый для перераспределения сообщений по новому ключу.

- [Repartition](screenshots/repartition.png)