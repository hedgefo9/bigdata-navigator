import json
from collections.abc import Callable
from typing import Any

from kafka import KafkaConsumer


class MetadataConsumer:
    def __init__(self, config: dict[str, Any]):
        kafka_config = config.get("kafka", {})
        topic = kafka_config.get("topic")
        if not topic:
            raise ValueError("kafka.topic is required")

        bootstrap_servers = kafka_config.get("bootstrap_servers", ["localhost:9092"])
        if isinstance(bootstrap_servers, str):
            bootstrap_servers = [bootstrap_servers]

        self.topic = topic
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=kafka_config.get("group_id", "search-service"),
            client_id=kafka_config.get("client_id", "search-service-consumer"),
            auto_offset_reset=kafka_config.get("auto_offset_reset", "earliest"),
            enable_auto_commit=bool(kafka_config.get("enable_auto_commit", False)),
            max_poll_records=int(kafka_config.get("max_poll_records", 100)),
            request_timeout_ms=int(kafka_config.get("request_timeout_ms", 10000)),
            consumer_timeout_ms=int(kafka_config.get("consumer_timeout_ms", 1000)),
            key_deserializer=lambda value: value.decode("utf-8") if value else None,
            value_deserializer=self._deserialize_value,
        )

    @staticmethod
    def _deserialize_value(value: bytes | None) -> dict[str, Any] | None:
        if value is None:
            return None
        raw = value.decode("utf-8").strip()
        if not raw:
            return None
        return json.loads(raw)

    def consume_forever(self, handler: Callable[[dict[str, Any]], None]):
        if handler is None:
            raise ValueError("handler is required")

        print(f"Kafka consumer started. topic={self.topic}")
        try:
            while True:
                for message in self.consumer:
                    payload = message.value
                    if payload is None:
                        self.consumer.commit()
                        continue

                    try:
                        handler(payload)
                        self.consumer.commit()
                    except Exception as error:
                        print(
                            "Failed to process message: "
                            f"topic={message.topic} partition={message.partition} "
                            f"offset={message.offset} error={error}"
                        )
        except KeyboardInterrupt:
            print("Kafka consumer stopped by user")
        finally:
            self.consumer.close()
