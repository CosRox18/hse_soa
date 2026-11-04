```mermaid
erDiagram
    AIRLINES ||--o{ FLIGHTS : operates
    FLIGHTS ||--o{ SEAT_RESERVATIONS : reserves
    FLIGHTS ||..o{ BOOKINGS : "flight_id, gRPC"
    BOOKINGS o|..|| SEAT_RESERVATIONS : "booking_id, gRPC"
    AIRLINES {
        text code PK
        text name
    }
    FLIGHTS {
        bigint id PK
        text flight_number
        text airline_code FK
        text origin
        text destination
        timestamptz departure_at
        timestamptz arrival_at
        integer total_seats
        integer available_seats
        numeric price
        text status
    }
    SEAT_RESERVATIONS {
        uuid booking_id PK
        bigint flight_id FK
        integer seat_count
        text status
        timestamptz created_at
    }
    CANCELLED_RESERVATIONS {
        uuid booking_id PK
    }
    BOOKINGS {
        uuid id PK
        bigint user_id
        bigint flight_id
        text passenger_name
        text passenger_email
        integer seat_count
        numeric total_price
        text status
        timestamptz created_at
    }
    RESERVATION_CLEANUP {
        uuid booking_id PK
        text purpose
        timestamptz created_at
    }
```

AIRLINES, FLIGHTS, SEAT_RESERVATIONS и CANCELLED_RESERVATIONS находятся в БД Flight Service. BOOKINGS и RESERVATION_CLEANUP — в БД Booking Service. Пунктирные связи проверяются через gRPC, внешних ключей между БД нет. У каждого сохранённого бронирования ровно одна резервация; после неудачной попытки может остаться RELEASED-резервация без бронирования.

Название авиакомпании вынесено в AIRLINES: оно зависит от её кода. У рейса уникальны номер и UTC-дата вылета. Названия аэропортов не дублируем, храним только IATA-коды. Данные пассажира относятся к конкретному бронированию, user_id не определяет пассажира. total_price — снимок стоимости бронирования. Во всех таблицах неключевые атрибуты зависят от ключа без транзитивных зависимостей.

RESERVATION_CLEANUP хранит незавершённые операции возврата мест. CANCELLED_RESERVATIONS запрещает запоздалому ReserveSeats создать резерв после компенсации.
