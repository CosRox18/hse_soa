from datetime import datetime, time, timedelta, timezone
import logging
import os
from pathlib import Path
import random
import time as clock
from uuid import NAMESPACE_URL, uuid4, uuid5

import requests


URL = os.environ.get('MOVIE_URL', 'http://movie:8000')
HEARTBEAT = Path('/tmp/generator-heartbeat')


def session(user, movie, start, key, complete=True):
    duration = 120 + int(uuid5(NAMESPACE_URL, key).int % 1800)
    steps = [('SEARCHED', 0), ('VIEW_STARTED', 0), ('VIEW_PAUSED', duration // 3), ('VIEW_RESUMED', duration // 3)]
    if complete:
        steps += [('VIEW_FINISHED', duration), ('LIKED', duration)]
    for index, (kind, progress) in enumerate(steps):
        event = {
            'event_id': str(uuid5(NAMESPACE_URL, f'{key}:{kind}')),
            'user_id': user, 'movie_id': '' if kind == 'SEARCHED' else movie,
            'event_type': kind, 'timestamp': (start + timedelta(seconds=progress + index)).isoformat(),
            'device_type': ['MOBILE', 'DESKTOP', 'TV', 'TABLET'][duration % 4],
            'session_id': key, 'progress_seconds': progress,
        }
        response = requests.post(f'{URL}/events', json=event, timeout=20)
        response.raise_for_status()
        HEARTBEAT.touch()


def seed(today):
    for offset in range(8, 0, -1):
        cohort = today - timedelta(days=offset)
        for user in range(10):
            name = f'demo-{cohort}-{user}'
            for age in range(min(offset, 7) + 1):
                day = cohort + timedelta(days=age)
                if day >= today or (age > 0 and user >= max(1, 8 - age)):
                    continue
                start = datetime.combine(day, time(10, user), timezone.utc)
                session(name, f'movie-{user % 4 + 1}', start, f'{name}:{day}', complete=user % 4 != 0)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    interval = float(os.environ.get('GENERATOR_INTERVAL_SECONDS', '5'))
    if interval <= 0:
        raise ValueError('GENERATOR_INTERVAL_SECONDS должен быть положительным')
    HEARTBEAT.touch()
    seeded = None
    while True:
        try:
            today = datetime.now(timezone.utc).date()
            if seeded != today:
                seed(today)
                seeded = today
                logging.info('seed_completed date=%s', today)
            session(f'demo-live-{random.randrange(20)}', f'movie-{random.randrange(1, 5)}',
                    datetime.now(timezone.utc) - timedelta(hours=1), str(uuid4()))
        except requests.RequestException:
            logging.exception('generation_failed')
        clock.sleep(interval)
