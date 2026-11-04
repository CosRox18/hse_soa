CREATE TABLE airlines (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE flights (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    flight_number TEXT NOT NULL,
    airline_code TEXT NOT NULL REFERENCES airlines(code),
    origin TEXT NOT NULL CHECK (origin ~ '^[A-Z]{3}$'),
    destination TEXT NOT NULL CHECK (destination ~ '^[A-Z]{3}$' AND destination <> origin),
    departure_at TIMESTAMPTZ NOT NULL,
    arrival_at TIMESTAMPTZ NOT NULL CHECK (arrival_at > departure_at),
    total_seats INTEGER NOT NULL CHECK (total_seats > 0),
    available_seats INTEGER NOT NULL CHECK (available_seats >= 0 AND available_seats <= total_seats),
    price NUMERIC(12, 2) NOT NULL CHECK (price > 0),
    status TEXT NOT NULL CHECK (status IN ('SCHEDULED', 'DEPARTED', 'CANCELLED', 'COMPLETED'))
);

CREATE UNIQUE INDEX flights_number_date ON flights
    (flight_number, ((departure_at AT TIME ZONE 'UTC')::date));
CREATE INDEX flights_route ON flights(origin, destination, departure_at)
    WHERE status = 'SCHEDULED';

CREATE TABLE seat_reservations (
    booking_id UUID PRIMARY KEY,
    flight_id BIGINT NOT NULL REFERENCES flights(id),
    seat_count INTEGER NOT NULL CHECK (seat_count > 0),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'RELEASED', 'EXPIRED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE cancelled_reservations (
    booking_id UUID PRIMARY KEY
);

INSERT INTO airlines VALUES ('SU', 'Аэрофлот');
INSERT INTO flights (flight_number, airline_code, origin, destination,
    departure_at, arrival_at, total_seats, available_seats, price, status)
VALUES
    ('SU100', 'SU', 'SVO', 'LED', CURRENT_DATE + INTERVAL '1 day 10 hours',
     CURRENT_DATE + INTERVAL '1 day 12 hours', 10, 10, 3500, 'SCHEDULED'),
    ('SU101', 'SU', 'LED', 'SVO', CURRENT_DATE + INTERVAL '1 day 15 hours',
     CURRENT_DATE + INTERVAL '1 day 17 hours', 20, 20, 4000, 'SCHEDULED');
