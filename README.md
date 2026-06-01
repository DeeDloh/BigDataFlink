# Highload Lab 3 — Flink Streaming

Лабораторная реализует потоковую обработку данных:

CSV → Producer → Kafka → Flink → PostgreSQL

Flink читает JSON-сообщения из Kafka, преобразует данные в модель "звезда" и записывает результат в PostgreSQL.

## Запуск

Сначала полностью очищаем старые контейнеры и данные:

```bash
docker compose down -v
````

Запускаем PostgreSQL, Kafka и создание Kafka-топика:

```bash
docker compose up -d --build postgres kafka kafka-init
```

Проверяем, что топик создался:

```bash
docker compose logs kafka-init
```

В логах должен быть топик:

```text
mock-data
```

Запускаем Flink:

```bash
docker compose up -d --build flink-jobmanager flink-taskmanager flink-submit
```

После этого запускаем producer, который отправит CSV-данные в Kafka:

```bash
docker compose up --build producer
```

## Проверка

Запустить проверочные SQL-запросы:

```bash
docker compose exec -T postgres psql -U postgres -d lab3 < queries/checks.sql
```

Ожидаемый результат:

```text
dim_customers | 10000
dim_sellers   | 10000
dim_products  | 10000
dim_stores    | 10000
dim_suppliers | 10000
dim_dates     | 364
fact_sales    | 10000
```

Также можно зайти в Flink UI:

```text
http://localhost:8081
```

PostgreSQL доступен на порту:

```text
localhost:5433
```

Данные для подключения:

```text
database: lab3
user: postgres
password: postgres
```

## Остановка

```bash
docker compose down
```



# BigDataFlink
Анализ больших данных - лабораторная работа №3 - Streaming processing с помощью Flink

Одним из самых популярных фреймворков для работы со streaming processing является Apache Flink. Apache Flink - мощный фреймворк, который предлагает широкий набор функциональности для простого написания streaming processing.

Что необходимо сделать? 

Необходимо реализовать потоковую обработку данных с помощью Flink, который читает топик Kafka, трансформирует данные в режиме streaming в модель звезда и пишет результат в PostgreSQL. Данные в Kafka-топиках хранятся в формате json. Данные в топик kafka нужно отправлять самостоятельно, эмулируя источник данных.

Какие данные отправляются в Kafka?
 - Каждое сообщение в Kafka-топике - это строчка из csv файлов, преобразованная в формат json.

Какие данные отправляются в PostgreSQL?
 - Трансформированные данные в модель данных звезда.

![Лабораторная работа №3](https://github.com/user-attachments/assets/d3c1544d-3fe6-4c15-b673-9aa5d27dbd76)


Алгоритм:

1. Клонируете к себе этот репозиторий.
2. Устанавливаете инструмент для работы с запросами SQL (рекомендую DBeaver).
3. Устанавливаете базу данных PostgreSQL (рекомендую установку через docker).
4. Устанавливаете Apache Flink (рекомендую установку через Docker).
5. Устанавливаете Apache Kafka (рекомендую установку через Docker).
6. Скачиваете файлы с исходными данными mock_data( * ).csv, где ( * ) номера файлов. Всего 10 файлов, каждый по 1000 строк.
7. Реализуете приложение, которое каждую строчку из исходных csv-файлов преобразует в json и отправляет в виде сообщения в Kafka-топик.
8. Реализуете приложение на Flink, которое читает Kafka-топик, преобразует данные в модель звезда и сохраняет в PostgreSQL в режиме streaming.
9. Проверяете конечные данные в PostgreSQL.
10. Отправляете работу на проверку лаборантам.

Что должно быть результатом работы?

1. Репозиторий, в котором есть исходные данные mock_data().csv, где () номера файлов. Всего 10 файлов, каждый по 1000 строк.
2. Файл docker-compose.yml с установкой PostgreSQL, Flink, Kafka и запуском приложения, которое из файлов mock_data(*).csv создает сообщения json в Kafka.
3. Инструкция, как запускать Flink-джобу и приложение для отправки данных в Kafka для проверки лабораторной работы.
4. Код Apache Flink для трансформации данных в режиме streaming.