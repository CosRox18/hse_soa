from contextlib import asynccontextmanager
from datetime import timezone
import logging
import os
from pathlib import Path
from threading import Event, Thread
from uuid import UUID
from typing import Literal

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from fastapi import FastAPI, HTTPException, Request
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator


TOPIC = 'movie-events'
BROKERS = os.environ['KAFKA_BROKERS']
REGISTRY_URL = os.environ['SCHEMA_REGISTRY_URL']


class MovieEvent(BaseModel):
    model_config = ConfigDict(extra='forbid')
    event_id: UUID
    user_id: str = Field(min_length=1, max_length=255)
    movie_id: str = Field(max_length=255)
    event_type: Literal['VIEW_STARTED', 'VIEW_FINISHED', 'VIEW_PAUSED', 'VIEW_RESUMED', 'LIKED', 'SEARCHED']
    timestamp: AwareDatetime
    device_type: Literal['MOBILE', 'DESKTOP', 'TV', 'TABLET']
    session_id: str = Field(min_length=1, max_length=255)
    progress_seconds: int = Field(default=0, ge=0, le=2147483647, strict=True)

    @field_validator('timestamp')
    @classmethod
    def utc_timestamp(cls, value):
        value = value.astimezone(timezone.utc)
        if not 1970 <= value.year <= 2100:
            raise ValueError('Год должен быть от 1970 до 2100')
        return value

    @model_validator(mode='after')
    def movie_required(self):
        if self.event_type != 'SEARCHED' and not self.movie_id:
            raise ValueError('Для события фильма нужен movie_id')
        return self


def poll(producer, stopped):
    while not stopped.is_set():
        producer.poll(0.1)


@asynccontextmanager
async def lifespan(app):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    admin = AdminClient({'bootstrap.servers': BROKERS})
    future = admin.create_topics([NewTopic(
        TOPIC, num_partitions=3, replication_factor=3,
        config={'min.insync.replicas': '2', 'retention.ms': '604800000', 'segment.bytes': '16777216'},
    )])[TOPIC]
    try:
        future.result()
    except KafkaException as error:
        if error.args[0].name() != 'TOPIC_ALREADY_EXISTS':
            raise
    registry = SchemaRegistryClient({'url': REGISTRY_URL})
    schema = Schema(Path('schemas/movie-event.avsc').read_text(), 'AVRO')
    registry.set_compatibility(level='BACKWARD', subject_name=f'{TOPIC}-value')
    registry.register_schema(f'{TOPIC}-value', schema)
    registered = registry.lookup_schema(f'{TOPIC}-value', schema)
    logging.info('schema_registered subject=%s version=%s id=%s', registered.subject, registered.version, registered.schema_id)
    app.state.serializer = AvroSerializer(registry, schema.schema_str, conf={'auto.register.schemas': False})
    app.state.producer = Producer({
        'bootstrap.servers': BROKERS, 'acks': 'all', 'enable.idempotence': True,
        'retry.backoff.ms': 100, 'retry.backoff.max.ms': 1600,
        'delivery.timeout.ms': 10000, 'request.timeout.ms': 3000,
    })
    app.state.admin = admin
    stopped = Event()
    worker = Thread(target=poll, args=(app.state.producer, stopped), daemon=True)
    worker.start()
    yield
    app.state.producer.flush(10)
    stopped.set()
    worker.join(timeout=2)


app = FastAPI(title='Movie Service', lifespan=lifespan)


@app.get('/health')
def health(request: Request):
    try:
        request.app.state.admin.list_topics(timeout=3)
    except KafkaException as error:
        raise HTTPException(503, str(error))
    return {'status': 'ok'}


@app.post('/events', status_code=201)
def publish(event: MovieEvent, request: Request):
    completed = Event()
    result = {}

    def delivered(error, message):
        result['error'] = error
        if error is None:
            result.update(partition=message.partition(), offset=message.offset())
        completed.set()

    value = event.model_dump(mode='json')
    value['timestamp'] = int(event.timestamp.timestamp() * 1000)
    try:
        data = request.app.state.serializer(value, SerializationContext(TOPIC, MessageField.VALUE))
        request.app.state.producer.produce(TOPIC, key=event.user_id.encode(), value=data, on_delivery=delivered)
    except Exception:
        logging.exception('publication_failed event_id=%s', event.event_id)
        raise HTTPException(503, 'Не удалось опубликовать событие')
    if not completed.wait(12) or result.get('error') is not None:
        logging.error('publication_failed event_id=%s reason=%s', event.event_id, result.get('error', 'timeout'))
        raise HTTPException(503, 'Kafka не подтвердила запись')
    logging.info('published event_id=%s event_type=%s timestamp=%s', event.event_id, event.event_type, event.timestamp.isoformat())
    return {'event_id': str(event.event_id), **{k: v for k, v in result.items() if k != 'error'}}
