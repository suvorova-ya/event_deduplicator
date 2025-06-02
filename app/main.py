from aiokafka import AIOKafkaProducer
from fastapi import FastAPI
from app.api.routers import router
from contextlib import asynccontextmanager




#Создаем продюсера один раз при старте приложения
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.producer = AIOKafkaProducer(
        bootstrap_servers="kafka:29092",
        linger_ms=100,

    )
    await app.state.producer.start()
    yield
    await app.state.producer.stop()
app = FastAPI(lifespan=lifespan)

app.include_router(router)