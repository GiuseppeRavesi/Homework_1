CREATE TABLE IF NOT EXISTS airports (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    airport_code TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS flights (
    id SERIAL PRIMARY KEY,
    airport_code TEXT,
    flight_number TEXT,
    departure_time BIGINT,
    arrival_time BIGINT
);