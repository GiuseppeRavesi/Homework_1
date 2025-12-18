from confluent_kafka import Producer
import json
import os

class FlightEventProducer:
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.topic = os.getenv("KAFKA_TOPIC")

        if not self.bootstrap_servers or not self.topic:
            raise RuntimeError("Kafka env vars not set")

        self.producer = None 

    def _get_producer(self):
        if self.producer is None:
            self.producer = Producer({
                "bootstrap.servers": self.bootstrap_servers,
                "client.id": "data-collector"
            })
        return self.producer

    def send_event(self, event: dict):
        producer = self._get_producer()
        producer.produce(
            self.topic,
            json.dumps(event).encode("utf-8")
        )
        producer.poll(0)
        producer.flush()
        print("[Kafka] Evento inviato", flush=True)

