Booking Service принимает REST-запросы и вызывает Flight Service по gRPC. У каждого своя PostgreSQL, миграции применяет Flyway. Контракт в `proto/flight.proto`, Python-код генерируем через protoc при сборке в `generated`, результат исключён из git. Схема БД и пояснение 3NF — в `er.md`.

Места проверяем под `SELECT FOR UPDATE`, уменьшение остатка и создание резерва делаем в одной транзакции. `booking_id` уникален: повтор ReserveSeats с теми же параметрами возвращает существующий резерв. При отмене возвращаем места только из ACTIVE. Цену фиксируем в бронировании, считаем через Decimal в рублях; в proto и JSON передаём деньги строкой, чтобы не терять точность.

Перед резервированием записываем намерение в reservation_cleanup. После успешной записи бронирования удаляем его в той же транзакции. При сбое возвращаем места; если сервис недоступен, фоновый обработчик повторит возврат. Отмена тоже сохраняется до gRPC-вызова. cancelled_reservations не даёт запоздалому ReserveSeats восстановить уже отменённый резерв. Между БД нет общей транзакции: незавершённые операции доводим до согласованного состояния после восстановления связи.

Flight Service проверяет x-api-key для всех RPC. Ключ задаётся через GRPC_API_KEY в окружении обоих контейнеров. Клиентский interceptor в Booking Service делает максимум три попытки только для UNAVAILABLE и DEADLINE_EXCEEDED, с паузами 100 и 200 мс. Там же circuit breaker: CLOSED, OPEN, один пробный запрос HALF_OPEN. Порог, окно ошибок и время OPEN задаются через CB_FAILURE_THRESHOLD, CB_WINDOW_SECONDS и CB_OPEN_SECONDS. gRPC-таймаут — GRPC_TIMEOUT_SECONDS. OPEN возвращает HTTP 503 без обращения к Flight Service.

Redis работает с мастером, репликой и Sentinel, клиент находит мастер через Sentinel. Кешируем GetFlight и SearchFlights через Cache-Aside с TTL 300 секунд. После UpdateFlight, ReserveSeats и ReleaseReservation удаляем ключи и меняем их поколение. Проверка поколения не даёт запоздалому чтению заполнить новый кеш старыми данными. При перезапуске Flight Service пространство ключей новое. Этот вариант рассчитан на один экземпляр Flight Service, как в compose. При недоступности Redis читаем PostgreSQL, поколение всё равно меняется, старые ключи истекают по TTL; hit/miss и переходы circuit breaker видны в логах.

```sh
docker compose up --build -d --wait
docker compose logs flight-migrate booking-migrate
curl 'http://127.0.0.1:18084/flights?origin=SVO&destination=LED'
```

В начальной миграции два рейса, первый SVO–LED, 10 мест по 3500 рублей. Swagger — <http://127.0.0.1:18084/docs>. Межсервисный ключ и пароли в compose заданы учебными значениями, можно переопределить переменными окружения.
