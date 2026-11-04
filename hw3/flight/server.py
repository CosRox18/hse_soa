from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from functools import wraps
import hmac
import logging
import os
import re
from uuid import UUID

import grpc
from google.protobuf.empty_pb2 import Empty
import psycopg

from db import connect
from flight.cache import FlightCache
import flight_pb2 as pb
import flight_pb2_grpc as rpc


FLIGHT_QUERY = 'SELECT f.*, a.name AS airline_name FROM flights f JOIN airlines a ON a.code = f.airline_code'


def flight_message(row):
    message = pb.Flight(
        id=row['id'], flight_number=row['flight_number'], airline_code=row['airline_code'],
        airline_name=row['airline_name'], origin=row['origin'], destination=row['destination'],
        total_seats=row['total_seats'], available_seats=row['available_seats'],
        price=str(row['price']), status=pb.FlightStatus.Value(row['status']),
    )
    message.departure_at.FromDatetime(row['departure_at'])
    message.arrival_at.FromDatetime(row['arrival_at'])
    return message


def reservation_message(row):
    return pb.SeatReservation(
        booking_id=str(row['booking_id']), flight_id=row['flight_id'],
        seat_count=row['seat_count'], status=pb.ReservationStatus.Value(row['status']),
    )


def handle_errors(method):
    @wraps(method)
    def wrapped(self, request, context):
        try:
            return method(self, request, context)
        except (ValueError, OverflowError) as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        except psycopg.Error:
            logging.exception('flight_database_error')
            context.abort(grpc.StatusCode.UNAVAILABLE, 'База рейсов недоступна')
    return wrapped


class ApiKeyAuth(grpc.ServerInterceptor):
    def intercept_service(self, continuation, details):
        handler = continuation(details)
        key = dict(details.invocation_metadata).get('x-api-key', '')
        if hmac.compare_digest(key, os.environ['GRPC_API_KEY']):
            return handler
        def denied(request, context):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, 'Нужен корректный x-api-key')
        return grpc.unary_unary_rpc_method_handler(
            denied, request_deserializer=handler.request_deserializer if handler else None,
            response_serializer=handler.response_serializer if handler else None,
        )


class FlightService(rpc.FlightServiceServicer):
    def __init__(self):
        self.cache = FlightCache()

    @handle_errors
    def GetFlight(self, request, context):
        if request.id <= 0:
            raise ValueError('id должен быть положительным')
        def load():
            with connect() as db:
                row = db.execute(FLIGHT_QUERY + ' WHERE f.id = %s', (request.id,)).fetchone()
            if row is None:
                context.abort(grpc.StatusCode.NOT_FOUND, 'Рейс не найден')
            return flight_message(row)
        return self.cache.get(f'flight:{request.id}', pb.Flight, load)

    @handle_errors
    def SearchFlights(self, request, context):
        if not all(re.fullmatch('[A-Z]{3}', value) for value in [request.origin, request.destination]):
            raise ValueError('Аэропорты должны быть IATA-кодами из трёх заглавных букв')
        day = request.date.ToDatetime(tzinfo=timezone.utc).date() if request.HasField('date') else None
        def load():
            sql = FLIGHT_QUERY + " WHERE f.origin = %s AND f.destination = %s AND f.status = 'SCHEDULED'"
            params = [request.origin, request.destination]
            if day:
                start = datetime.combine(day, datetime.min.time(), timezone.utc)
                sql += ' AND f.departure_at >= %s AND f.departure_at < %s'
                params += [start, start + timedelta(days=1)]
            with connect() as db:
                rows = db.execute(sql + ' ORDER BY f.departure_at, f.id', params).fetchall()
            return pb.SearchFlightsResponse(flights=[flight_message(row) for row in rows])
        return self.cache.get(f'search:{request.origin}:{request.destination}:{day}', pb.SearchFlightsResponse, load)

    @handle_errors
    def ReserveSeats(self, request, context):
        booking_id = UUID(request.booking_id)
        if request.flight_id <= 0 or request.seat_count <= 0:
            raise ValueError('flight_id и seat_count должны быть положительными')
        with connect() as db:
            db.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))', (str(booking_id),))
            old = db.execute('SELECT * FROM seat_reservations WHERE booking_id = %s', (booking_id,)).fetchone()
            if old:
                if old['flight_id'] != request.flight_id or old['seat_count'] != request.seat_count:
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, 'booking_id уже использован с другими параметрами')
                if old['status'] != 'ACTIVE':
                    context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Резервация уже закрыта')
                self.cache.invalidate()
                return reservation_message(old)
            cancelled = db.execute('SELECT 1 FROM cancelled_reservations WHERE booking_id = %s', (booking_id,)).fetchone()
            if cancelled:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Бронирование уже отменено')
            flight = db.execute('SELECT * FROM flights WHERE id = %s FOR UPDATE', (request.flight_id,)).fetchone()
            if flight is None:
                context.abort(grpc.StatusCode.NOT_FOUND, 'Рейс не найден')
            if flight['status'] != 'SCHEDULED':
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Рейс недоступен для бронирования')
            if flight['available_seats'] < request.seat_count:
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, 'Недостаточно мест')
            db.execute('UPDATE flights SET available_seats = available_seats - %s WHERE id = %s', (request.seat_count, request.flight_id))
            row = db.execute(
                "INSERT INTO seat_reservations (booking_id, flight_id, seat_count, status) VALUES (%s, %s, %s, 'ACTIVE') RETURNING *",
                (booking_id, request.flight_id, request.seat_count),
            ).fetchone()
        self.cache.invalidate()
        return reservation_message(row)

    @handle_errors
    def ReleaseReservation(self, request, context):
        booking_id = UUID(request.booking_id)
        with connect() as db:
            db.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))', (str(booking_id),))
            db.execute('INSERT INTO cancelled_reservations VALUES (%s) ON CONFLICT DO NOTHING', (booking_id,))
            row = db.execute('SELECT * FROM seat_reservations WHERE booking_id = %s FOR UPDATE', (booking_id,)).fetchone()
            if row and row['status'] == 'ACTIVE':
                db.execute('SELECT id FROM flights WHERE id = %s FOR UPDATE', (row['flight_id'],))
                db.execute('UPDATE flights SET available_seats = available_seats + %s WHERE id = %s', (row['seat_count'], row['flight_id']))
                db.execute("UPDATE seat_reservations SET status = 'RELEASED' WHERE booking_id = %s", (booking_id,))
        if row:
            self.cache.invalidate()
        return Empty()

    @handle_errors
    def UpdateFlight(self, request, context):
        if request.id <= 0 or not (request.HasField('price') or request.HasField('status')):
            raise ValueError('Нужны id и хотя бы одно изменяемое поле')
        fields, params = [], []
        if request.HasField('price'):
            try:
                price = Decimal(request.price)
                if not price.is_finite() or not 0 < price <= Decimal('9999999999.99') or price != price.quantize(Decimal('0.01')):
                    raise ValueError('Цена должна быть положительной с точностью 0.01')
            except InvalidOperation:
                raise ValueError('Невалидная цена')
            fields.append('price = %s')
            params.append(price)
        if request.HasField('status'):
            if request.status not in [pb.SCHEDULED, pb.DEPARTED, pb.CANCELLED, pb.COMPLETED]:
                raise ValueError('Невалидный статус рейса')
            fields.append('status = %s')
            params.append(pb.FlightStatus.Name(request.status))
        with connect() as db:
            row = db.execute('UPDATE flights SET ' + ', '.join(fields) + ' WHERE id = %s RETURNING id', [*params, request.id]).fetchone()
            if row is None:
                context.abort(grpc.StatusCode.NOT_FOUND, 'Рейс не найден')
            row = db.execute(FLIGHT_QUERY + ' WHERE f.id = %s', (request.id,)).fetchone()
        self.cache.invalidate()
        return flight_message(row)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    server = grpc.server(ThreadPoolExecutor(max_workers=10), interceptors=[ApiKeyAuth()])
    rpc.add_FlightServiceServicer_to_server(FlightService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    logging.info('Flight Service listening on 50051')
    server.wait_for_termination()
