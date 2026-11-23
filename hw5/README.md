# ДЗ 5

```bash
docker compose up --build -d --wait
docker compose --profile test run --build --rm tests
```

Movie Service: `http://localhost:18085/docs`, Aggregation Service: `http://localhost:18086/docs`, Grafana: `http://localhost:18087/d/cinema`.


## События

Movie Service принимает JSON, проверяет его и публикует Avro в `movie-events`. Схема хранится в Karapace, совместимость — `BACKWARD`. Ответ приходит после подтверждения Kafka.

В топике три партиции, фактор репликации 3, `min.insync.replicas=2`. Три брокера также образуют кворум KRaft. Ключ — `user_id`: события одного пользователя попадают в одну партицию. Producer использует `acks=all`, идемпотентность и повторы с экспоненциальной задержкой.

```bash
curl -X POST localhost:18085/events -H 'Content-Type: application/json' -d '{
  "event_id": "90c7f714-644d-4392-bc2e-798fd2149cce",
  "user_id": "user-1",
  "movie_id": "movie-1",
  "event_type": "VIEW_STARTED",
  "timestamp": "2026-11-23T10:00:00Z",
  "device_type": "DESKTOP",
  "session_id": "session-1",
  "progress_seconds": 0
}'
```

`generator` отправляет события через HTTP: поиск, начало просмотра, пауза, продолжение, завершение, лайк. Сначала создаёт историю за восемь завершённых дней, затем новые сессии каждые 5 секунд. Интервал задаётся через `GENERATOR_INTERVAL_SECONDS`.

## Обработка

ClickHouse читает Kafka через Kafka Engine и materialized view. События сохраняются в `analytics.events`: `ReplacingMergeTree`, партиции по месяцам, сортировка по дате, пользователю и UUID. Повторная доставка одного неизменяемого события с тем же UUID не влияет на агрегаты: запросы используют `FINAL`.

Aggregation Service сам применяет SQL из `migrations`, создаёт S3-бакет и запускает пересчёт. Период задаётся через `AGGREGATION_INTERVAL_SECONDS`, по умолчанию 60 секунд. Каждый цикл пересчитывает текущий день и восемь предыдущих; более старые даты можно пересчитать вручную. Все даты считаются в UTC.

Метрики за день:

- DAU — число пользователей с любым событием;
- среднее время просмотра — среднее `progress_seconds` у `VIEW_FINISHED`;
- топ-10 фильмов — по числу `VIEW_STARTED`;
- конверсия — число `VIEW_FINISHED` / число `VIEW_STARTED`;
- D1 и D7 — доля пользователей, вернувшихся ровно через 1 и 7 дней после первого `VIEW_STARTED`. Возвратом считается любое событие.

Нет завершений или начал — среднее или конверсия равны `NULL`. Для retention нет когорты — `NULL`, нет возвратов у созревшей когорты — 0. Будущие дни когорты остаются пустыми.

Снимок агрегатов хранится в `analytics.daily_metrics`. PostgreSQL получает строки с датой, названием метрики, измерением, значением и временем расчёта. Повторный расчёт заменяет данные за дату; в PostgreSQL замена выполняется транзакцией. Общей транзакции между ClickHouse и PostgreSQL нет: неудачный расчёт нужно повторить, для последних дней это делает планировщик.

Grafana читает агрегаты из ClickHouse. На дашборде — когортная таблица Day 0–7, DAU и конверсия. Таблица показывает состояние на последний завершённый день.

После расчёта сервис читает метрики из PostgreSQL и выгружает JSON в S3-совместимый SeaweedFS:

```text
s3://movie-analytics/daily/YYYY-MM-DD/aggregates.json
```

Повторный экспорт перезаписывает объект. Неудачные экспорты остаются в PostgreSQL в `export_queue` и повторяются в следующем цикле, в том числе для старых дат. Сбой S3 не отменяет успешный расчёт: `/aggregate` возвращает `export_key: null`. Ошибки, начало и конец расчёта, число обработанных событий и время выполнения пишутся в логи.

```bash
curl -X POST 'localhost:18086/aggregate?date=2026-11-22'
curl -X POST 'localhost:18086/export?date=2026-11-22'
docker compose logs --tail 30 aggregation
docker compose exec postgres psql -U analytics -d analytics -c "SELECT * FROM daily_metrics WHERE day = '2026-11-22';"
docker compose exec clickhouse clickhouse-client --password clickhouse-dev --query 'SELECT * FROM analytics.daily_metrics FINAL ORDER BY day FORMAT Vertical'
```
