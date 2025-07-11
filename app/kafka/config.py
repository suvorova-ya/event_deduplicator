import os
from typing import Final

from aiokafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor

# Настройки Kafka
KAFKA_BOOTSTRAP: Final[str] = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC_NAME: Final[str] = os.environ["KAFKA_TOPIC"]
NUM_CONSUMERS: Final[int] = int(os.environ["NUM_CONSUMERS"])

# Настройки продюсера
PRODUCER_CONFIG: Final[dict] = {
    "bootstrap_servers": KAFKA_BOOTSTRAP,
    "linger_ms": 100,
    "acks": "all",
    "enable_idempotence": True,
    "compression_type": "gzip"
}

# Настройки консьюмера
CONSUMER_CONFIG: Final[dict] = {
    "bootstrap_servers": KAFKA_BOOTSTRAP,
    "auto_offset_reset": "earliest",
    "group_id": "products_consumer_group",
    "max_poll_records": 500,
    "fetch_max_bytes": 10485760,
    "fetch_max_wait_ms": 500,
    "enable_auto_commit": False,
    "session_timeout_ms": 30000,
    "heartbeat_interval_ms": 5000,
    "max_poll_interval_ms": 300000,
    "partition_assignment_strategy": [RoundRobinPartitionAssignor]
}