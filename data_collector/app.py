from flask import Flask, request, jsonify
import os
from datetime import datetime, timedelta, timezone

from grpc_client import UserManagerClient
from opensky_client import OpenSkyClient
from database import fetch_user_airports, save_flight_record, get_connection

from apscheduler.schedulers.background import BackgroundScheduler
from circuit_breaker import CircuitBreakerOpen
from kafka_producer import FlightEventProducer

app = Flask(__name__)

def validate_thresholds(high_value, low_value):
    if high_value is not None and high_value < 0:
        raise ValueError("high_value must be >= 0")

    if low_value is not None and low_value < 0:
        raise ValueError("low_value must be >= 0")

    if high_value is not None and low_value is not None:
        if high_value <= low_value:
            raise ValueError("high_value must be greater than low_value")


# -------------------------
#   CONFIGURAZIONE
# -------------------------
COLLECTION_INTERVAL_HOURS = int(os.getenv("COLLECTION_INTERVAL_HOURS", 12))

user_client = UserManagerClient()
opensky = OpenSkyClient()
event_producer = None


# ============================================================
#   FUNZIONE PRINCIPALE DI RACCOLTA DATI (scheduler + manuale)
# ============================================================

def collect_flight_data():
    print("[Scheduler] Raccolta dati iniziata...")


    global event_producer
    if event_producer is None:
        event_producer = FlightEventProducer()
        
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT email FROM airports")
        emails = [r[0] for r in cur.fetchall()]
    conn.close()

    print(f"[Scheduler] Trovati {len(emails)} utenti con aeroporti")

    total_saved = 0

    for email in emails:
        exists, _ = user_client.user_exists(email)
        if not exists:
            print(f"[Scheduler] Utente {email} non esiste più → ignorato")
            continue

        airports = fetch_user_airports(email)

        for airport in airports:
            print(f"[Scheduler] Recupero voli per {email} @ {airport}")

            try:
                flights = opensky.get_flights_for_airport(airport)
            except CircuitBreakerOpen:
                print(
                    f"[OpenSky] Circuit breaker OPEN → salto aeroporto {airport}",
                    flush=True
                )
                continue
            except Exception as e:
                print(
                    f"[OpenSky] Errore recupero voli per {airport}: {e}",
                    flush=True
                )
                continue

            # --- Salvataggio partenze ---
            for f in flights["departures"]:
                save_flight_record(
                    email=email,
                    airport_code=airport,
                    flight_type="departure",
                    callsign=f.get("callsign"),
                    icao24=f.get("icao24"),
                    first_seen=f.get("firstSeen"),
                    last_seen=f.get("lastSeen"),
                    origin_country=f.get("estDepartureAirport")
                )
                total_saved += 1

            # --- Salvataggio arrivi ---
            for f in flights["arrivals"]:
                save_flight_record(
                    email=email,
                    airport_code=airport,
                    flight_type="arrival",
                    callsign=f.get("callsign"),
                    icao24=f.get("icao24"),
                    first_seen=f.get("firstSeen"),
                    last_seen=f.get("lastSeen"),
                    origin_country=f.get("estDepartureAirport")
                )
                total_saved += 1

            event = {
                "email": email,
                "airport": airport,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "departures": len(flights.get("departures", [])),
                "arrivals": len(flights.get("arrivals", []))
            }

            try:
                event_producer.send_event(event)
                print(
                    f"[Kafka] Evento inviato → email={email}, airport={airport}, "
                    f"dep={event['departures']}, arr={event['arrivals']}",
                    flush=True)
            except Exception as e:
                print(f"[Kafka] Errore invio evento: {e}", flush=True)

    print(f"[Scheduler] Raccolta completata → {total_saved} nuovi record salvati")
    return total_saved


# ======================================
#   ENDPOINT DI BASE / SANITA’ DEL SERVIZIO
# ======================================

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True}), 200


@app.route("/", methods=["GET"])
def root():
    return jsonify({"service": "data_collector", "status": "running"}), 200


# ======================================
#   GESTIONE INTERESSI (airports)
# ======================================

@app.route("/airports", methods=["POST"])
def add_airport():
    data = request.get_json()

    email = data.get("email")
    airport = data.get("airport_code", "").upper()
    high_value = data.get("high_value")
    low_value = data.get("low_value")

    try:
        validate_thresholds(high_value, low_value)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    if not email or not airport:
        return jsonify({"error": "email e airport_code richiesti"}), 400

    exists, _ = user_client.user_exists(email)
    if not exists:
        return jsonify({"error": "utente non esiste"}), 404

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
                """
                INSERT INTO airports (email, airport_code, high_value, low_value)
                VALUES (%s, %s, %s, %s)
                """,
        (email, airport, high_value, low_value)
)
        conn.commit()
    conn.close()

    return jsonify({
        "message": "Aeroporto aggiunto con successo",
        "email": email,
        "airport": airport
    }), 201


@app.route("/airports/<email>", methods=["GET"])
def list_airports(email):
    """Restituisce tutti gli aeroporti di interesse dell’utente."""
    airports = fetch_user_airports(email)
    return jsonify({"email": email, "airports": airports}), 200

@app.route("/airports", methods=["DELETE"])
def delete_airport():
    email = request.args.get("email")
    airport = request.args.get("airport_code", "").upper()

    if not email or not airport:
        return jsonify({"error": "email e airport_code richiesti"}), 400

    exists, _ = user_client.user_exists(email)
    if not exists:
        return jsonify({"error": "utente non esiste"}), 404

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM airports WHERE email=%s AND airport_code=%s",
            (email, airport)
        )
        found = cur.fetchone()

        if not found:
            conn.close()
            return jsonify({"error": "interesse non trovato"}), 404

        cur.execute(
            "DELETE FROM airports WHERE email=%s AND airport_code=%s",
            (email, airport)
        )
        conn.commit()

    conn.close()

    return jsonify({
        "message": "Interesse eliminato con successo",
        "email": email,
        "airport": airport
    }), 200


@app.route("/airports/preferences", methods=["PUT"])
def update_airport_preferences():
    data = request.get_json()

    email = data.get("email")
    airport = data.get("airport_code", "").upper()
    high_value = data.get("high_value")
    low_value = data.get("low_value")

    if not email or not airport:
        return jsonify({"error": "email e airport_code richiesti"}), 400

    exists, _ = user_client.user_exists(email)
    if not exists:
        return jsonify({"error": "utente non esiste"}), 404

    try:
        validate_thresholds(high_value, low_value)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE airports
            SET high_value=%s, low_value=%s
            WHERE email=%s AND airport_code=%s
            """,
            (high_value, low_value, email, airport)
        )
        if cur.rowcount == 0:
            conn.close()
            return jsonify({"error": "interesse non trovato"}), 404
        conn.commit()
    conn.close()

    return jsonify({
        "status": "ok",
        "email": email,
        "airport": airport,
        "high_value": high_value,
        "low_value": low_value
    }), 200


# ======================================
#   RACCOLTA MANUALE
# ======================================

@app.route("/collect", methods=["POST"])
def collect_manual():
    saved = collect_flight_data()
    return jsonify({"saved": saved}), 200


# ======================================
#   LETTURA VOLI
# ======================================

@app.route("/flights/<airport>", methods=["GET"])
def flights_list(airport):
    email = request.args.get("email")
    if not email:
        return jsonify({"error": "email obbligatoria"}), 400

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT flight_type, callsign, icao24, first_seen, last_seen, created_at
            FROM flights
            WHERE email=%s AND airport_code=%s
            ORDER BY created_at DESC
            LIMIT 200
        """, (email, airport.upper()))
        rows = cur.fetchall()
    conn.close()

    res = []
    for r in rows:
        res.append({
            "flight_type": r[0],
            "callsign": r[1],
            "icao24": r[2],
            "first_seen": r[3],
            "last_seen": r[4],
            "created_at": str(r[5]),
        })

    return jsonify({"airport": airport, "email": email, "flights": res}), 200


# ======================================
#   ULTIMO VOLO (latest)
# ======================================

@app.route("/flights/<airport>/latest", methods=["GET"])
def flights_latest(airport):
    email = request.args.get("email")
    if not email:
        return jsonify({"error": "email obbligatoria"}), 400

    airport = airport.upper()

    conn = get_connection()
    with conn.cursor() as cur:
        # ultimo volo in PARTENZA
        cur.execute("""
            SELECT callsign, icao24, first_seen, last_seen, created_at
            FROM flights
            WHERE email=%s AND airport_code=%s AND flight_type='departure'
            ORDER BY created_at DESC
            LIMIT 1
        """, (email, airport))
        dep = cur.fetchone()

        # ultimo volo in ARRIVO
        cur.execute("""
            SELECT callsign, icao24, first_seen, last_seen, created_at
            FROM flights
            WHERE email=%s AND airport_code=%s AND flight_type='arrival'
            ORDER BY created_at DESC
            LIMIT 1
        """, (email, airport))
        arr = cur.fetchone()

    conn.close()

    if not dep and not arr:
        return jsonify({"message": "nessun volo trovato"}), 404

    response = {"airport": airport, "email": email}

    if dep:
        response["latest_departure"] = {
            "callsign": dep[0],
            "icao24": dep[1],
            "first_seen": dep[2],
            "last_seen": dep[3],
            "created_at": str(dep[4])
        }

    if arr:
        response["latest_arrival"] = {
            "callsign": arr[0],
            "icao24": arr[1],
            "first_seen": arr[2],
            "last_seen": arr[3],
            "created_at": str(arr[4])
        }

    return jsonify(response), 200



# ======================================
#   MEDIA ULTIMI X GIORNI
# ======================================

@app.route("/flights/<airport>/average", methods=["GET"])
def flights_average(airport):
    email = request.args.get("email")
    days = int(request.args.get("days", 7))

    since = datetime.utcnow() - timedelta(days=days)

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT flight_type, COUNT(*)
            FROM flights
            WHERE email=%s AND airport_code=%s AND created_at >= %s
            GROUP BY flight_type
        """, (email, airport.upper(), since))
        rows = cur.fetchall()
    conn.close()

    return jsonify({
        "airport": airport,
        "email": email,
        "days": days,
        "stats": rows
    }), 200


# ======================================
#   SCHEDULER AUTOMATICO (ogni X ore)
# ======================================

scheduler = BackgroundScheduler()
scheduler.add_job(collect_flight_data, "interval", hours=COLLECTION_INTERVAL_HOURS)
scheduler.start()


# ======================================
#   MAIN
# ======================================

if __name__ == "__main__":
    print(f"[DataCollector] Avvio server con scheduler ogni {COLLECTION_INTERVAL_HOURS} ore")
    app.run(host="0.0.0.0", port=5001)
