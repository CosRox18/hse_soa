CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.events (
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3, 'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id String,
    progress_seconds Int32,
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (toDate(timestamp), user_id, event_id);

CREATE TABLE IF NOT EXISTS analytics.kafka_events (
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3, 'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id String,
    progress_seconds Int32
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    kafka_topic_list = 'movie-events', kafka_group_name = 'clickhouse-movie-events',
    kafka_format = 'AvroConfluent', format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 3, kafka_thread_per_consumer = 1,
    kafka_max_block_size = 100, kafka_flush_interval_ms = 500;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.ingest_events
TO analytics.events AS SELECT * FROM analytics.kafka_events;

CREATE TABLE IF NOT EXISTS analytics.daily_metrics (
    day Date,
    dau UInt64,
    avg_watch_seconds Nullable(Float64),
    conversion Nullable(Float64),
    retention_d1 Nullable(Float64),
    retention_d7 Nullable(Float64),
    top_movies Map(String, UInt64),
    cohorts Array(Tuple(cohort_date Date, day_number UInt8, retention Nullable(Float64))),
    processed UInt64,
    computed_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY day;
