from app.logging_config import logger
import os
import asyncio
import signal
from app.deduplicator.deduplicator import Deduplicator
from aiokafka import AIOKafkaConsumer
from app.kafka.consumer import consume
import backoff
from aiokafka.errors import KafkaConnectionError


NUM_CONSUMERS = int(os.getenv("NUM_CONSUMERS", "5"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "products_events")

@backoff.on_exception(
    backoff.expo,
    KafkaConnectionError,
    max_tries=20,      # Можно увеличить число попыток
    max_time=180       # И время ожидания, чтобы Kafka точно успела подняться
)
async def start_consumer(consumer):
    await consumer.start()

# Для корректной остановки по SIGTERM/SIGINT (docker stop)
stop_event = asyncio.Event()

def handle_stop(*_):
    logger.warning("Получен сигнал остановки, завершаем работу...")
    stop_event.set()

async def main():
    # Для Docker graceful shutdown
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGTERM, handle_stop)
    loop.add_signal_handler(signal.SIGINT, handle_stop)

    deduplicator = Deduplicator()
    await deduplicator.init_bloom_filter()
    consumer_objects = []
    consumer_tasks = []

    for i in range(NUM_CONSUMERS):
        consumer = AIOKafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="earliest",
            group_id='products_consumer_group',
            max_poll_records=100,
            enable_auto_commit=False
        )
        await start_consumer(consumer)
        task = asyncio.create_task(consume(consumer, deduplicator))
        consumer_objects.append(consumer)
        consumer_tasks.append(task)

    await stop_event.wait()
    logger.warning("Останавливаем консьюмеры...")

    for task in consumer_tasks:
        task.cancel()
    await asyncio.gather(*consumer_tasks, return_exceptions=True)
    for consumer in consumer_objects:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())





