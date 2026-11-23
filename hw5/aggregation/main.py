from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
from threading import Event, Lock, Thread
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import clickhouse_connect
from fastapi import FastAPI, HTTPException, Query
import psycopg
from psycopg.rows import dict_row


LOCK = Lock()
BUCKET = 'movie-analytics'


def postgres():
    return psycopg.connect(os.environ['DATABASE_URL'], row_factory=dict_row, connect_timeout=3)


def clickhouse(database='analytics'):
    return clickhouse_connect.get_client(
        host='clickhouse', database=database, username='default',
        password=os.environ['CLICKHOUSE_PASSWORD'], connect_timeout=3,
        send_receive_timeout=30, settings={'max_threads': 2, 'max_memory_usage': 268435456},
    )


def s3():
    return boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT_URL'], config=Config(
        connect_timeout=3, read_timeout=10, retries={'max_attempts': 2, 'mode': 'standard'},
        s3={'addressing_style': 'path'},
    ))


def migrate():
    with postgres() as db:
        db.execute(Path('migrations/postgres/001_init.sql').read_text())
    with clickhouse('default') as db:
        for sql in Path('migrations/clickhouse/001_init.sql').read_text().split(';'):
            if sql.strip():
                db.command(sql)
    client = s3()
    try:
        client.head_bucket(Bucket=BUCKET)
    except ClientError as error:
        if error.response['ResponseMetadata']['HTTPStatusCode'] != 404:
            raise
        client.create_bucket(Bucket=BUCKET)


def query(db, name, day):
    return db.query(Path(f'aggregation/{name}.sql').read_text(), parameters={'day': day}).result_rows


def save_postgres(rows, day):
    for attempt in range(3):
        try:
            with postgres() as db:
                db.execute('DELETE FROM daily_metrics WHERE day = %s', (day,))
                with db.cursor() as cursor:
                    cursor.executemany(
                        'INSERT INTO daily_metrics (day, metric, dimension, value, computed_at) VALUES (%s, %s, %s, %s, %s)', rows,
                    )
                db.execute('INSERT INTO export_queue VALUES (%s) ON CONFLICT DO NOTHING', (day,))
            return
        except psycopg.Error:
            logging.exception('postgres_retry date=%s attempt=%s', day, attempt + 1)
            if attempt == 2:
                raise
            time.sleep(0.2 * 2 ** attempt)


def export_day(day):
    with postgres() as db:
        rows = db.execute('SELECT * FROM daily_metrics WHERE day = %s ORDER BY metric, dimension', (day,)).fetchall()
        if not rows:
            raise ValueError('За эту дату нет агрегатов')
        db.execute('INSERT INTO export_queue VALUES (%s) ON CONFLICT DO NOTHING', (day,))
    payload = [dict(row, day=row['day'].isoformat(), computed_at=row['computed_at'].isoformat()) for row in rows]
    key = f'daily/{day.isoformat()}/aggregates.json'
    s3().put_object(Bucket=BUCKET, Key=key, Body=json.dumps(payload, ensure_ascii=False, allow_nan=False).encode(),
                    ContentType='application/json')
    with postgres() as db:
        db.execute('DELETE FROM export_queue WHERE day = %s', (day,))
    logging.info('export_completed date=%s rows=%s key=%s', day, len(rows), key)
    return key


def aggregate(day):
    started = time.monotonic()
    logging.info('aggregation_started date=%s', day)
    computed_at = datetime.now(timezone.utc)
    with clickhouse() as db:
        dau, average, conversion, processed = query(db, 'daily', day)[0]
        movies = dict(query(db, 'top', day))
        cohorts = query(db, 'cohorts', day)
        retention = {(cohort, age): value for cohort, age, value in cohorts}
        d1 = retention.get((day - timedelta(days=1), 1))
        d7 = retention.get((day - timedelta(days=7), 7))
        db.insert('daily_metrics', [[day, dau, average, conversion, d1, d7, movies, cohorts, processed, computed_at]],
                  column_names=['day', 'dau', 'avg_watch_seconds', 'conversion', 'retention_d1', 'retention_d7',
                                'top_movies', 'cohorts', 'processed', 'computed_at'])
    metrics = [('dau', '', dau), ('avg_watch_seconds', '', average), ('conversion', '', conversion),
               ('retention_d1', '', d1), ('retention_d7', '', d7)]
    metrics += [('top_movie', movie, views) for movie, views in movies.items()]
    metrics += [(f'cohort_day_{age}', cohort.isoformat(), value) for cohort, age, value in cohorts]
    rows = [(day, name, dimension, float(value) if value is not None else None, computed_at)
            for name, dimension, value in metrics]
    save_postgres(rows, day)
    logging.info('aggregation_completed date=%s events=%s metrics=%s seconds=%.3f',
                 day, processed, len(rows), time.monotonic() - started)
    try:
        key = export_day(day)
    except Exception:
        logging.exception('export_pending date=%s', day)
        key = None
    return {'date': day, 'processed': processed, 'metrics': len(rows), 'export_key': key}


def scheduled(stopped, interval):
    while not stopped.is_set():
        try:
            with postgres() as db:
                pending = db.execute('SELECT day FROM export_queue ORDER BY day').fetchall()
            for row in pending:
                try:
                    with LOCK:
                        export_day(row['day'])
                except Exception:
                    logging.exception('export_pending date=%s', row['day'])
        except psycopg.Error:
            logging.exception('export_queue_unavailable')
        today = datetime.now(timezone.utc).date()
        for offset in range(8, -1, -1):
            if stopped.is_set():
                return
            try:
                with LOCK:
                    aggregate(today - timedelta(days=offset))
            except Exception:
                logging.exception('aggregation_failed date=%s', today - timedelta(days=offset))
        stopped.wait(interval)


@asynccontextmanager
async def lifespan(app):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    interval = float(os.environ.get('AGGREGATION_INTERVAL_SECONDS', '60'))
    if interval <= 0:
        raise ValueError('AGGREGATION_INTERVAL_SECONDS должен быть положительным')
    migrate()
    stopped = Event()
    worker = Thread(target=scheduled, args=(stopped, interval), daemon=True)
    worker.start()
    yield
    stopped.set()
    worker.join(timeout=5)


app = FastAPI(title='Aggregation Service', lifespan=lifespan)


def valid_day(day):
    if day > datetime.now(timezone.utc).date() or day.year < 1970:
        raise HTTPException(422, 'Дата должна быть от 1970 года до текущего дня UTC')


@app.get('/health')
def health():
    with postgres() as db:
        db.execute('SELECT 1')
    with clickhouse() as db:
        db.command('SELECT 1')
    return {'status': 'ok'}


@app.post('/aggregate')
def recalculate(date: date = Query()):
    valid_day(date)
    try:
        with LOCK:
            return aggregate(date)
    except Exception:
        logging.exception('manual_aggregation_failed date=%s', date)
        raise HTTPException(503, 'Пересчёт не завершён, повторите запрос после восстановления сервисов')


@app.post('/export')
def export(date: date = Query()):
    valid_day(date)
    try:
        with LOCK:
            return {'key': export_day(date)}
    except ValueError as error:
        raise HTTPException(404, str(error))
    except Exception:
        logging.exception('export_failed date=%s', date)
        raise HTTPException(503, 'Экспорт не завершён; сохранённые задания повторяются по расписанию')
