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
    "linger_ms": 50,
    "acks": 1,  #сли же требуется максимальная надежность, применяют acks=all
    "max_request_size":1048576,
    "max_batch_size": 64 * 1024,
    "enable_idempotence": True,
    "compression_type": "lz4",
    "num_network_threads": min(8, os.cpu_count() or 4),
    "queued_max_requests": 1000,
    "max_in_flight_requests_per_connection": 3,
    "max.in.flight":1,
    "request_timeout_ms": 60000,
    "retry_backoff_ms": 200,
    "metadata_max_age_ms": 300000,
}

# Настройки консьюмера
CONSUMER_CONFIG: Final[dict] = {
    "bootstrap_servers": KAFKA_BOOTSTRAP,
    "auto_offset_reset": "earliest",
    "group_id": "products_consumer_group",
    "max_poll_records": 500,
    "fetch_max_bytes": 10485760,
    "fetch_min_bytes": 65536,
    "fetch_max_wait_ms": 1000,
    "enable_auto_commit": False,
    "num_network_threads": min(8, os.cpu_count() or 4),
    "session_timeout_ms": 30000,
    "heartbeat_interval_ms": 5000,
    "max_poll_interval_ms": 300000,
    "partition_assignment_strategy": [RoundRobinPartitionAssignor]
}