from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
import logging
from threading import Event, Thread
from typing import Annotated, Literal
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
import grpc
from pydantic import BaseModel, ConfigDict, Field
import psycopg

from booking.client import CircuitOpen, FlightClient
from db import connect
import flight_pb2 as pb


PositiveId = Annotated[int, Field(gt=0, le=9223372036854775807)]


class BookingCreate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    user_id: PositiveId
    flight_id: PositiveId
    passenger_name: str = Field(min_length=1, max_length=255, pattern=r'^[^\x00]*$')
    passenger_email: str = Field(max_length=255, pattern=r'^[^@\s\x00]+@[^@\s\x00]+\.[^@\s\x00]+$')
    seat_count: int = Field(gt=0, le=2147483647)


class BookingResponse(BaseModel):
    id: UUID
    user_id: int
    flight_id: int
    passenger_name: str
    passenger_email: str
    seat_count: int
    total_price: Decimal
    status: Literal['CONFIRMED', 'CANCELLED']
    created_at: datetime


class FlightResponse(BaseModel):
    id: int
    flight_number: str
    airline_code: str
    airline_name: str
    origin: str
    destination: str
    departure_at: datetime
    arrival_at: datetime
    total_seats: int
    available_seats: int
    price: Decimal
    status: str


def flight_response(flight):
    return {
        'id': flight.id, 'flight_number': flight.flight_number,
        'airline_code': flight.airline_code, 'airline_name': flight.airline_name,
        'origin': flight.origin, 'destination': flight.destination,
        'departure_at': flight.departure_at.ToDatetime(tzinfo=timezone.utc),
        'arrival_at': flight.arrival_at.ToDatetime(tzinfo=timezone.utc),
        'total_seats': flight.total_seats, 'available_seats': flight.available_seats,
        'price': Decimal(flight.price), 'status': pb.FlightStatus.Name(flight.status),
    }


def cleanup_one(client, booking_id=None):
    with connect() as db:
        if booking_id is None:
            job = db.execute(
                "SELECT * FROM reservation_cleanup WHERE created_at < now() - interval '5 seconds' "
                'ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED',
            ).fetchone()
        else:
            job = db.execute('SELECT * FROM reservation_cleanup WHERE booking_id = %s FOR UPDATE', (booking_id,)).fetchone()
        if job is None:
            return False
        client.stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=str(job['booking_id'])))
        if job['purpose'] == 'CANCEL':
            db.execute("UPDATE bookings SET status = 'CANCELLED' WHERE id = %s", (job['booking_id'],))
        db.execute('DELETE FROM reservation_cleanup WHERE booking_id = %s', (job['booking_id'],))
    logging.info('reservation_cleanup booking_id=%s purpose=%s', job['booking_id'], job['purpose'])
    return True


def cleanup_loop(client, stopped):
    while not stopped.wait(1):
        try:
            cleanup_one(client)
        except (grpc.RpcError, CircuitOpen, psycopg.Error) as error:
            logging.warning('reservation_cleanup_pending reason=%s', type(error).__name__)


@asynccontextmanager
async def lifespan(app):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    client = FlightClient()
    app.state.flight = client
    stopped = Event()
    worker = Thread(target=cleanup_loop, args=(client, stopped), daemon=True)
    worker.start()
    yield
    stopped.set()
    client.close()
    worker.join(timeout=5)


app = FastAPI(title='Flight Booking', lifespan=lifespan)


@app.exception_handler(grpc.RpcError)
async def grpc_error(request, error):
    codes = {
        grpc.StatusCode.INVALID_ARGUMENT: 400,
        grpc.StatusCode.NOT_FOUND: 404,
        grpc.StatusCode.RESOURCE_EXHAUSTED: 409,
        grpc.StatusCode.FAILED_PRECONDITION: 409,
        grpc.StatusCode.UNAVAILABLE: 503,
        grpc.StatusCode.DEADLINE_EXCEEDED: 503,
    }
    return JSONResponse({'detail': error.details() or error.code().name}, status_code=codes.get(error.code(), 502))


@app.exception_handler(CircuitOpen)
async def circuit_error(request, error):
    return JSONResponse({'detail': str(error)}, status_code=503)


@app.exception_handler(psycopg.Error)
async def database_error(request, error):
    logging.error('booking_database_error type=%s', type(error).__name__)
    return JSONResponse({'detail': 'База бронирований недоступна'}, status_code=503)


@app.get('/health')
def health():
    with connect() as db:
        db.execute('SELECT 1')
    return {'status': 'ok'}


@app.get('/flights', response_model=list[FlightResponse])
def search_flights(request: Request, origin: Annotated[str, Query(pattern='^[A-Z]{3}$')],
                   destination: Annotated[str, Query(pattern='^[A-Z]{3}$')], date: date | None = None):
    query = pb.SearchFlightsRequest(origin=origin, destination=destination)
    if date:
        query.date.FromDatetime(datetime.combine(date, datetime.min.time(), timezone.utc))
    return [flight_response(flight) for flight in request.app.state.flight.stub.SearchFlights(query).flights]


@app.get('/flights/{id}', response_model=FlightResponse)
def get_flight(id: PositiveId, request: Request):
    return flight_response(request.app.state.flight.stub.GetFlight(pb.GetFlightRequest(id=id)))


def find_booking(db, id):
    booking = db.execute('SELECT * FROM bookings WHERE id = %s', (id,)).fetchone()
    if booking is None:
        raise HTTPException(404, 'Бронирование не найдено')
    return booking


@app.post('/bookings', response_model=BookingResponse, status_code=201)
def create_booking(body: BookingCreate, request: Request):
    client = request.app.state.flight
    flight = client.stub.GetFlight(pb.GetFlightRequest(id=body.flight_id))
    id = uuid4()
    with connect() as db:
        db.execute("INSERT INTO reservation_cleanup (booking_id, purpose) VALUES (%s, 'ROLLBACK')", (id,))
    try:
        with connect() as db:
            job = db.execute('SELECT booking_id FROM reservation_cleanup WHERE booking_id = %s FOR UPDATE', (id,)).fetchone()
            if job is None:
                raise HTTPException(503, 'Попытка бронирования истекла, повторите запрос')
            client.stub.ReserveSeats(pb.ReserveSeatsRequest(flight_id=body.flight_id, seat_count=body.seat_count, booking_id=str(id)))
            booking = db.execute(
                """INSERT INTO bookings (id, user_id, flight_id, passenger_name, passenger_email,
                seat_count, total_price, status) VALUES (%s, %s, %s, %s, %s, %s, %s, 'CONFIRMED') RETURNING *""",
                (id, body.user_id, body.flight_id, body.passenger_name, body.passenger_email,
                 body.seat_count, body.seat_count * Decimal(flight.price)),
            ).fetchone()
            db.execute('DELETE FROM reservation_cleanup WHERE booking_id = %s', (id,))
        return booking
    except (grpc.RpcError, CircuitOpen, psycopg.Error):
        try:
            cleanup_one(client, id)
        except (grpc.RpcError, CircuitOpen, psycopg.Error):
            logging.warning('reservation_cleanup_deferred booking_id=%s', id)
        raise


@app.get('/bookings', response_model=list[BookingResponse])
def list_bookings(user_id: Annotated[int, Query(gt=0, le=9223372036854775807)]):
    with connect() as db:
        return db.execute('SELECT * FROM bookings WHERE user_id = %s ORDER BY created_at, id', (user_id,)).fetchall()


@app.get('/bookings/{id}', response_model=BookingResponse)
def get_booking(id: UUID):
    with connect() as db:
        return find_booking(db, id)


@app.post('/bookings/{id}/cancel', response_model=BookingResponse)
def cancel_booking(id: UUID, request: Request):
    with connect() as db:
        booking = find_booking(db, id)
        if booking['status'] != 'CONFIRMED':
            raise HTTPException(409, 'Отменять можно только CONFIRMED')
        db.execute("INSERT INTO reservation_cleanup (booking_id, purpose) VALUES (%s, 'CANCEL') ON CONFLICT DO NOTHING", (id,))
    cleanup_one(request.app.state.flight, id)
    with connect() as db:
        return find_booking(db, id)
