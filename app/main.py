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
    try:
        yield
    finally:
        await producer.stop()



app = FastAPI(lifespan=lifespan)

app.include_router(router)








