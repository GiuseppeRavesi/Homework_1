CREATE TABLE IF NOT EXISTS airports (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    airport_code TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS flights (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    airport_code TEXT NOT NULL,
    flight_type TEXT NOT NULL,        
    callsign TEXT,
    icao24 TEXT,
    first_seen BIGINT,                
    last_seen BIGINT,                
    origin_country TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_flights_airport ON flights(airport_code);
CREATE INDEX IF NOT EXISTS idx_flights_email ON flights(email);
CREATE INDEX IF NOT EXISTS idx_flights_created_at ON flights(created_at);
