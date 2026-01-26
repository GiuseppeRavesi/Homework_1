from kafka_consumer import AlertEventConsumer
from kafka_producer import AlertProducer
import psycopg2
import os
import time

from prometheus_client import Counter, Gauge, start_http_server

SERVICE_NAME = os.getenv("SERVICE_NAME", "alert_system")
NODE_NAME = os.getenv("NODE_NAME", "node_1")

# MINI SERVER DI METRICS PROMETHEUS
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

# =========================
# PROMETHEUS METRICS
# =========================

kafka_messages_consumed_total = Counter(
    "kafka_messages_consumed_total",
    "Total Kafka messages consumed by alert_system",
    ["service", "node", "topic", "result"]  # result: ok|error|empty
)

kafka_last_message_processing_seconds = Gauge(
    "kafka_last_message_processing_seconds",
    "Seconds spent processing the last Kafka message",
    ["service", "node", "topic"]
)

db_threshold_queries_total = Counter(
    "db_threshold_queries_total",
    "Total DB queries for thresholds",
    ["service", "node", "result"]  # ok|error|not_found
)

alerts_sent_total = Counter(
    "alerts_sent_total",
    "Total alerts sent to notifier",
    ["service", "node", "topic", "result"]  # ok|error
)


def get_thresholds(email, airport):
    start = time.time()
    try:
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

        if row is None:
            db_threshold_queries_total.labels(
                service=SERVICE_NAME, node=NODE_NAME, result="not_found"
            ).inc()
        else:
            db_threshold_queries_total.labels(
                service=SERVICE_NAME, node=NODE_NAME, result="ok"
            ).inc()

        return row

    except Exception as e:
        db_threshold_queries_total.labels(
            service=SERVICE_NAME, node=NODE_NAME, result="error"
        ).inc()
        print(f"[AlertSystem][DB][ERROR] {e}", flush=True)
        return None

def main():

    start_http_server(METRICS_PORT, addr="0.0.0.0")
    print(f"[AlertSystem] Metrics server on :{METRICS_PORT}/metrics", flush=True)
    
    consumer = AlertEventConsumer()
    producer = AlertProducer()

    in_topic = getattr(consumer, "topic", "unknown")
    out_topic = getattr(producer, "topic", "unknown")

    print("[AlertSystem] Avviato", flush=True)

    while True:

        poll_start = time.time()

        event = consumer.poll()
        if not event:
            kafka_messages_consumed_total.labels(
                service=SERVICE_NAME, node=NODE_NAME, topic=in_topic, result="empty"
            ).inc()
            continue
        
        try:
            email = event["email"]
            airport = event["airport"]
            departures = event["departures"]
            arrivals = event["arrivals"]

            kafka_messages_consumed_total.labels(
                service=SERVICE_NAME, node=NODE_NAME, topic=in_topic, result="ok"
            ).inc()

        except Exception as e:
            kafka_messages_consumed_total.labels(
                service=SERVICE_NAME, node=NODE_NAME, topic=in_topic, result="error"
            ).inc()
            print(f"[AlertSystem][EVENT][ERROR] bad event format: {e} | event={event}", flush=True)
            continue
        finally:
            kafka_last_message_processing_seconds.labels(
                service=SERVICE_NAME, node=NODE_NAME, topic=in_topic
            ).set(time.time() - poll_start)

        #DB thresholds
        thresholds = get_thresholds(email, airport)
        if not thresholds:
            continue

        high, low = thresholds

        # Alert logic + produce
        if high is not None and departures > high:
            try:
                producer.send_alert({
                    "email": email,
                    "airport": airport,
                    "condition": f"SUPERATA SOGLIA ALTA: {departures} > {high}"
                })
                alerts_sent_total.labels(
                    service=SERVICE_NAME, node=NODE_NAME, topic=out_topic, result="ok"
                ).inc()
            except Exception as e:
                alerts_sent_total.labels(
                    service=SERVICE_NAME, node=NODE_NAME, topic=out_topic, result="error"
                ).inc()
                print(f"[AlertSystem][ALERT][ERROR] {e}", flush=True)

        if low is not None and arrivals < low:
            try:
                producer.send_alert({
                    "email": email,
                    "airport": airport,
                    "condition": f"SOTTO SOGLIA BASSA: {arrivals} < {low}"
                })
                alerts_sent_total.labels(
                    service=SERVICE_NAME, node=NODE_NAME, topic=out_topic, result="ok"
                ).inc()
            except Exception as e:
                alerts_sent_total.labels(
                    service=SERVICE_NAME, node=NODE_NAME, topic=out_topic, result="error"
                ).inc()
                print(f"[AlertSystem][ALERT][ERROR] {e}", flush=True)


if __name__ == "__main__":
    main()
