import os

import psycopg
from fastapi import Request
from psycopg.rows import dict_row


def connect():
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def get_db(request: Request):
    with connect() as db:
        request.state.db = db
        yield db
