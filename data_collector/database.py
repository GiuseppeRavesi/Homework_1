import os
import psycopg2
from psycopg2.extras import RealDictCursor

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DATA_DB_HOST", "data_db"),
        port=int(os.getenv("DATA_DB_PORT", 5432)),
        database=os.getenv("DATA_DB_NAME", "flights"),
        user=os.getenv("DATA_DB_USER", "postgres"),
        password=os.getenv("DATA_DB_PASSWORD", "postgres")
    )

def fetch_user_airports(email):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT airport_code FROM airports WHERE email=%s", (email,))
            rows = cur.fetchall()
            return [r[0] for r in rows]
    finally:
        conn.close()

def save_flight_record(email, airport_code, flight_type, callsign, icao24, first_seen, last_seen, origin_country):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO flights (email, airport_code, flight_type, callsign, icao24, first_seen, last_seen, origin_country)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, (email, airport_code, flight_type, callsign, icao24, first_seen, last_seen, origin_country))
            conn.commit()
    finally:
        conn.close()
