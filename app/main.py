from app.logging_config import logger, perf_logger
import os
from app.deduplicator.deduplicator import Deduplicator
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from fastapi import FastAPI
from app.api.routers import router
from app.kafka.consumer import  consume
from contextlib import asynccontextmanager
import backoff
import asyncio
from app.logging_config import logger


NUM_CONSUMERS = int(os.getenv("NUM_CONSUMERS", "5"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "products_events")

@backoff.on_exception(
    backoff.expo,
    KafkaConnectionError,
    max_tries=10,
    max_time=60
)

async def connect_to_kafka_producer() -> AIOKafkaProducer:
    """
    Попытаться подключиться к Kafka и вернуть уже запущенный producer.
    Если не получилось — backoff будет делать retry.
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        linger_ms=100,
    )
    await producer.start()
    logger.info(" Kafka Producer успешно запущен")
    return producer



@asynccontextmanager
async def lifespan(app: FastAPI):
    await asyncio.sleep(5)

    producer = await connect_to_kafka_producer()
    app.state.producer = producer

    deduplicator = Deduplicator()
    await deduplicator.init_bloom_filter()
    app.state.deduplicator = deduplicator

    #  Порождаем несколько консьюмеров (NUM_CONSUMERS шт.) и соответствующие задачи в фоновом режиме.
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
        await consumer.start()
        # асинхронная задача, которая будет слушать топик и вызывать функцию consume()
        task = asyncio.create_task(consume(consumer, deduplicator))
        consumer_objects.append(consumer)
        consumer_tasks.append(task)
        logger.info(f"Запущен консьюмер #{i + 1} для топика «{TOPIC_NAME}»")


    app.state.consumers = consumer_objects  # Для корректного завершения
    app.state.consumer_tasks = consumer_tasks

    try:
        yield

    finally:
        # === shutdown logic ===

        # 1) Сначала отменяем все фоновые задачи (consume loop)
        for task in consumer_tasks:
            task.cancel()

        # Ждём, пока все таски отработают cancel()
        await asyncio.gather(*consumer_tasks, return_exceptions=True)

        # 2) Останавливаем каждого AIOKafkaConsumer (отпускаем сетевые ресурсы)
        for consumer in consumer_objects:
            try:
                await consumer.stop()
            except Exception as e:
                logger.warning(f"Ошибка при остановке консьюмера: {e}")

        logger.info("Все консьюмеры корректно остановлены")

        try:
            await producer.stop()
        except Exception as e:
            logger.warning(f"Ошибка при остановке продюсера: {e}")
        else:
            logger.info("Kafka Producer корректно остановлен")



app = FastAPI(lifespan=lifespan)

app.include_router(router)








