from kafka_consumer import AlertEventConsumer
from kafka_producer import AlertProducer
import psycopg2
import os

def get_thresholds(email, airport):
    conn = psycopg2.connect(
        host=os.getenv("DATA_DB_HOST"),
        database=os.getenv("DATA_DB_NAME"),
        user=os.getenv("DATA_DB_USER"),
        password=os.getenv("DATA_DB_PASSWORD")
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT high_value, low_value
        FROM airports
        WHERE email=%s AND airport_code=%s
    """, (email, airport))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

def main():
    consumer = AlertEventConsumer()
    producer = AlertProducer()

    print("[AlertSystem] Avviato", flush=True)

    while True:
        event = consumer.poll()
        if not event:
            continue

        email = event["email"]
        airport = event["airport"]
        departures = event["departures"]
        arrivals = event["arrivals"]

        thresholds = get_thresholds(email, airport)
        if not thresholds:
            continue

        high, low = thresholds

        if high is not None and departures > high:
            producer.send_alert({
                "email": email,
                "airport": airport,
                "condition": f"SUPERATA SOGLIA ALTA: {departures} > {high}"
            })

        if low is not None and arrivals < low:
            producer.send_alert({
                "email": email,
                "airport": airport,
                "condition": f"SOTTO SOGLIA BASSA: {arrivals} < {low}"
            })

if __name__ == "__main__":
    main()
