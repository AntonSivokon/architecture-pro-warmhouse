import json
from typing import Any, Protocol


COMMAND_TOPIC = "iot.device-command"


class CommandPublisher(Protocol):
    def publish(self, message: dict[str, Any]) -> None: ...


class KafkaCommandPublisher:
    def __init__(self, bootstrap_servers: str) -> None:
        from kafka import KafkaProducer

        self._producer = KafkaProducer(
            bootstrap_servers=[server.strip() for server in bootstrap_servers.split(",") if server.strip()],
            key_serializer=lambda key: key.encode("utf-8"),
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            request_timeout_ms=5000,
            max_block_ms=5000,
        )

    def publish(self, message: dict[str, Any]) -> None:
        future = self._producer.send(
            COMMAND_TOPIC,
            key=str(message["deviceId"]),
            value=message,
        )
        future.get(timeout=10)