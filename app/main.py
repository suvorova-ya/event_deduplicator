from app.logging_config import logger, perf_logger
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from fastapi import FastAPI
from app.api.routers import router
from contextlib import asynccontextmanager
import backoff
import asyncio
from app.logging_config import logger
from app.kafka.config import PRODUCER_CONFIG


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
    producer = AIOKafkaProducer(**PRODUCER_CONFIG)
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








