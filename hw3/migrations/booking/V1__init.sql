CREATE TABLE bookings (
    id UUID PRIMARY KEY,
    user_id BIGINT NOT NULL CHECK (user_id > 0),
    flight_id BIGINT NOT NULL CHECK (flight_id > 0),
    passenger_name TEXT NOT NULL,
    passenger_email TEXT NOT NULL,
    seat_count INTEGER NOT NULL CHECK (seat_count > 0),
    total_price NUMERIC(22, 2) NOT NULL CHECK (total_price > 0),
    status TEXT NOT NULL CHECK (status IN ('CONFIRMED', 'CANCELLED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX bookings_user ON bookings(user_id, created_at);

CREATE TABLE reservation_cleanup (
    booking_id UUID PRIMARY KEY,
    purpose TEXT NOT NULL CHECK (purpose IN ('ROLLBACK', 'CANCEL')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
