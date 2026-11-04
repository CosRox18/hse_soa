from collections import deque, namedtuple
import logging
import os
from threading import Lock
import time

import grpc

import flight_pb2_grpc as rpc


TRANSIENT = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}


def positive_setting(name, default, cast=float):
    value = cast(os.environ.get(name, default))
    if value <= 0:
        raise ValueError(f'{name} должен быть положительным')
    return value


class CircuitOpen(Exception):
    pass


class CircuitBreaker:
    def __init__(self):
        self.threshold = positive_setting('CB_FAILURE_THRESHOLD', '5', int)
        self.open_seconds = positive_setting('CB_OPEN_SECONDS', '10')
        self.window = positive_setting('CB_WINDOW_SECONDS', '30')
        self.state = 'CLOSED'
        self.failures = deque()
        self.opened_at = 0
        self.generation = 0
        self.lock = Lock()

    def transition(self, state):
        logging.info('circuit_breaker %s -> %s', self.state, state)
        self.state = state
        self.generation += 1
        if state == 'OPEN':
            self.opened_at = time.monotonic()

    def enter(self):
        with self.lock:
            if self.state == 'OPEN':
                if time.monotonic() - self.opened_at < self.open_seconds:
                    raise CircuitOpen('Flight Service временно недоступен: circuit breaker OPEN')
                self.transition('HALF_OPEN')
                return self.generation
            if self.state == 'HALF_OPEN':
                raise CircuitOpen('Проверяется восстановление Flight Service')
            return self.generation

    def finish(self, generation, failed):
        with self.lock:
            if generation != self.generation:
                return
            if not failed:
                self.failures.clear()
                if self.state == 'HALF_OPEN':
                    self.transition('CLOSED')
                return
            now = time.monotonic()
            while self.failures and now - self.failures[0] > self.window:
                self.failures.popleft()
            self.failures.append(now)
            if self.state == 'HALF_OPEN' or len(self.failures) >= self.threshold:
                self.transition('OPEN')


class CallDetails(namedtuple('CallDetails', 'method timeout metadata credentials wait_for_ready compression'), grpc.ClientCallDetails):
    pass


class FlightPolicy(grpc.UnaryUnaryClientInterceptor):
    def __init__(self):
        self.breaker = CircuitBreaker()
        self.timeout = positive_setting('GRPC_TIMEOUT_SECONDS', '1')

    def intercept_unary_unary(self, continuation, details, request):
        generation = self.breaker.enter()
        metadata = [*(details.metadata or []), ('x-api-key', os.environ['GRPC_API_KEY'])]
        details = CallDetails(details.method, self.timeout, metadata, details.credentials, False, details.compression)
        for attempt in range(3):
            call = continuation(details, request)
            try:
                call.result()
            except grpc.RpcError as error:
                transient = error.code() in TRANSIENT
                if transient and attempt < 2:
                    delay = 0.1 * 2 ** attempt
                    logging.warning('grpc_retry method=%s attempt=%s code=%s delay=%.1f', details.method, attempt + 1, error.code().name, delay)
                    time.sleep(delay)
                    continue
                self.breaker.finish(generation, transient)
            else:
                self.breaker.finish(generation, False)
            return call


class FlightClient:
    def __init__(self):
        self.channel = grpc.insecure_channel(os.environ.get('FLIGHT_ADDRESS', 'flight:50051'), options=[('grpc.enable_retries', 0)])
        self.stub = rpc.FlightServiceStub(grpc.intercept_channel(self.channel, FlightPolicy()))

    def close(self):
        self.channel.close()
