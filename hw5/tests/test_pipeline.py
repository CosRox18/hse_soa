from datetime import date, datetime, timedelta, timezone
import json
import time
from uuid import uuid4

from confluent_kafka.admin import AdminClient
import pytest
import requests

from aggregation.main import clickhouse, postgres, s3


MOVIE = 'http://movie:8000'
AGGREGATION = 'http://aggregation:8000'


@pytest.fixture
def dataset():
    prefix = f'hw5-test-{uuid4()}-'
    day = date(1970, 1, 1)
    db = clickhouse()
    while db.command('''SELECT count() FROM (
                         SELECT toDate(timestamp) AS day FROM events
                         UNION ALL SELECT day FROM daily_metrics
                       ) WHERE day IN {days:Array(Date)}''',
                     parameters={'days': [day, day + timedelta(days=1), day + timedelta(days=7)]}):
        day += timedelta(days=9)
    yield db, day, prefix
    days = [day + timedelta(days=offset) for offset in [0, 1, 7]]
    db.command('ALTER TABLE events DELETE WHERE startsWith(user_id, {prefix:String}) SETTINGS mutations_sync = 1',
               parameters={'prefix': prefix})
    db.command('ALTER TABLE daily_metrics DELETE WHERE day IN {days:Array(Date)} SETTINGS mutations_sync = 1',
               parameters={'days': days})
    with postgres() as pg:
        pg.execute('DELETE FROM daily_metrics WHERE day = ANY(%s)', (days,))
        pg.execute('DELETE FROM export_queue WHERE day = ANY(%s)', (days,))
    s3().delete_objects(Bucket='movie-analytics', Delete={
        'Objects': [{'Key': f'daily/{day}/aggregates.json'} for day in days],
    })
    db.close()


def event(day, user, kind, progress=0, movie='movie-test-a', second=0):
    return {
        'event_id': str(uuid4()), 'user_id': user, 'movie_id': movie,
        'event_type': kind, 'timestamp': (datetime.combine(day, datetime.min.time(), timezone.utc)
                                        + timedelta(hours=12, seconds=second)).isoformat(),
        'device_type': 'MOBILE', 'session_id': f'{user}-{day}', 'progress_seconds': progress,
    }


def publish(payload):
    response = requests.post(f'{MOVIE}/events', json=payload, timeout=20)
    response.raise_for_status()
    assert response.json()['event_id'] == payload['event_id']


def wait_for(db, ids):
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        count = db.command('SELECT count() FROM events FINAL WHERE event_id IN {ids:Array(UUID)}', parameters={'ids': ids})
        if count == len(ids):
            return
        time.sleep(0.2)
    pytest.fail('События не появились в ClickHouse')


def recalculate(day):
    response = requests.post(f'{AGGREGATION}/aggregate', params={'date': day}, timeout=60)
    response.raise_for_status()
    with postgres() as db:
        rows = db.execute('SELECT * FROM daily_metrics WHERE day = %s', (day,)).fetchall()
    return {(row['metric'], row['dimension']): row['value'] for row in rows}


def test_event_roundtrip(dataset):
    db, day, prefix = dataset
    payload = event(day, prefix + 'user', 'VIEW_STARTED')
    publish(payload)
    publish(payload)
    wait_for(db, [payload['event_id']])
    row = db.query('SELECT * FROM events FINAL WHERE event_id = {id:UUID}',
                   parameters={'id': payload['event_id']}).first_item
    assert str(row['event_id']) == payload['event_id']
    for key in ['user_id', 'movie_id', 'event_type', 'device_type', 'session_id', 'progress_seconds']:
        assert row[key] == payload[key]
    assert row['timestamp'].replace(tzinfo=timezone.utc).isoformat() == payload['timestamp']
    invalid = dict(payload, progress_seconds=-1)
    assert requests.post(f'{MOVIE}/events', json=invalid, timeout=10).status_code == 422
    metadata = AdminClient({'bootstrap.servers': 'kafka-1:9092,kafka-2:9092,kafka-3:9092'}).list_topics(timeout=10)
    topic = metadata.topics['movie-events']
    assert len(topic.partitions) >= 3
    assert all(len(partition.replicas) >= 2 for partition in topic.partitions.values())
    schema = requests.get('http://schema-registry:8081/subjects/movie-events-value/versions/latest', timeout=10).json()
    assert schema['version'] >= 1
    assert json.loads(schema['schema'])['name'] == 'MovieEvent'


def test_metrics_cohorts_and_export(dataset):
    db, day, prefix = dataset
    a, b = prefix + 'a', prefix + 'b'
    day1, day7 = day + timedelta(days=1), day + timedelta(days=7)
    events = [
        event(day, a, 'VIEW_STARTED'), event(day, a, 'VIEW_FINISHED', 120, second=120),
        event(day, b, 'VIEW_STARTED', movie='movie-test-b'),
        event(day, b, 'VIEW_PAUSED', 30, movie='movie-test-b', second=30),
        event(day, b, 'VIEW_RESUMED', 30, movie='movie-test-b', second=35),
        event(day1, a, 'SEARCHED', movie=''),
        event(day7, b, 'VIEW_STARTED', movie='movie-test-b'),
        event(day7, b, 'VIEW_FINISHED', 240, movie='movie-test-b', second=240),
    ]
    for payload in events:
        publish(payload)
    wait_for(db, [payload['event_id'] for payload in events])
    first = recalculate(day)
    assert first[('dau', '')] == 2
    assert first[('avg_watch_seconds', '')] == 120
    assert first[('conversion', '')] == 0.5
    assert first[('top_movie', 'movie-test-a')] == first[('top_movie', 'movie-test-b')] == 1
    assert first[('cohort_day_0', str(day))] == 1
    assert first[('cohort_day_1', str(day))] is None
    assert recalculate(day) == first
    assert db.command('SELECT count() FROM daily_metrics FINAL WHERE day = {day:Date}', parameters={'day': day}) == 1
    next_day = recalculate(day1)
    assert next_day[('retention_d1', '')] == 0.5
    assert next_day[('avg_watch_seconds', '')] is None
    assert next_day[('conversion', '')] is None
    week = recalculate(day7)
    assert week[('retention_d7', '')] == 0.5
    assert week[('cohort_day_2', str(day))] == 0
    assert week[('cohort_day_7', str(day))] == 0.5
    response = requests.post(f'{AGGREGATION}/export', params={'date': day7}, timeout=30)
    response.raise_for_status()
    key = f'daily/{day7}/aggregates.json'
    assert response.json()['key'] == key
    client = s3()
    data = json.loads(client.get_object(Bucket='movie-analytics', Key=key)['Body'].read())
    exported = {(row['metric'], row['dimension']): row['value'] for row in data}
    assert exported == week
    assert all(row['day'] == str(day7) and row['computed_at'] for row in data)
    assert len(client.list_objects_v2(Bucket='movie-analytics', Prefix=f'daily/{day7}/')['Contents']) == 1
