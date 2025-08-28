import logging
import time

from confluent_kafka import KafkaError, Producer

logger = logging.getLogger("producer")


class KafkaProducerWrapper:
    def __init__(self, settings, tuning):
        conf = {
            "bootstrap.servers": settings.bootstrap_servers,
            "enable.idempotence": tuning.enable_idempotence,
            "acks": "all",
            "max.in.flight.requests.per.connection": tuning.max_in_flight,
            "compression.type": tuning.compression,
            "linger.ms": tuning.linger_ms,
            "batch.size": tuning.batch_bytes,
            "retries": tuning.retries,
        }
        self.producer = Producer(conf)
        self.topic_raw = settings.topic_raw
        self.topic_error = settings.topic_error

    def delivery_callback(self, err, msg):
        if err:
            logger.error(
                "Message failed",
                extra={"error": str(err), "topic": msg.topic()},
            )
        else:
            logger.info(
                "Message delivered",
                extra={"topic": msg.topic(), "key": msg.key()},
            )

    def produce(self, topic, key, value):
        for attempt in range(5):  # simple retry
            try:
                self.producer.produce(
                    topic,
                    key=key,
                    value=value,
                    callback=self.delivery_callback,
                )
                self.producer.poll(0)
                return
            except KafkaError as e:
                logger.warning(f"Produce attempt {attempt+1} failed: {e}")
                time.sleep(0.5)
        logger.error(
            "Failed to produce after retries",
            extra={"topic": topic, "key": key, "value": value},
        )

    def flush(self):
        self.producer.flush(10)
