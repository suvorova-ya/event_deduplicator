from app.kafka.config import TOPIC_NAME, CONSUMER_CONFIG, NUM_CONSUMERS
from app.logging_config import logger
import asyncio
import signal
from app.deduplicator.deduplicator import Deduplicator
from aiokafka import AIOKafkaConsumer
from app.kafka.consumer import consume
import backoff
from aiokafka.errors import KafkaConnectionError



@backoff.on_exception(
    backoff.expo,
    KafkaConnectionError,
    max_tries=20,      # Можно увеличить число попыток
    max_time=180       # Время ожидания, чтобы Kafka точно успела подняться
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

    consumers = [AIOKafkaConsumer(TOPIC_NAME, **CONSUMER_CONFIG) for _ in range(NUM_CONSUMERS)]
    tasks = []

    try:
        for consumer in consumers:
            await start_consumer(consumer)
            tasks.append(asyncio.create_task(consume(consumer, deduplicator)))

        logger.info(f"Запущено {NUM_CONSUMERS} консьюмеров")
        await stop_event.wait()
        logger.warning("Останавливаем консьюмеры...")

    finally:
        logger.warning("Останавливаем консьюмеры...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        for consumer in consumers:
            await consumer.stop()
        logger.info("Все консьюмеры остановлены")





if __name__ == "__main__":
    asyncio.run(main())





