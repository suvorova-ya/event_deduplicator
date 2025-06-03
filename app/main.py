from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from fastapi import FastAPI
from app.api.routers import router
from app.kafka.consumer import start_consumer
from contextlib import asynccontextmanager
import backoff
import asyncio


@backoff.on_exception(
    backoff.expo,
    KafkaConnectionError,
    max_tries=10,
    max_time=60
)

async def connect_to_kafka():
    producer = AIOKafkaProducer(
        bootstrap_servers="kafka:9092",
        linger_ms=100
    )
    await producer.start()
    return producer


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.producer = await connect_to_kafka()
    print(" Kafka producer подключён")
    consumer_task = asyncio.create_task(start_consumer())
    print("Kafka consumer подключён")
    try:
        yield
    finally:
    # Завершение работы
        await app.state.producer.stop()
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            print("Kafka consumer stopped")


app = FastAPI(lifespan=lifespan)

app.include_router(router)