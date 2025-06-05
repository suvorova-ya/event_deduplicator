import asyncio

from app.api.schemas import EventCreate
from fastapi import Request
from app.logging_config import logger



async def send_to_kafka(request: Request, event: EventCreate):
    topic = 'products_events'
    send_event = event.model_dump_json().encode('utf-8')
    logger.info(f"Отправка события в Kafka: {send_event}")

    producer = request.app.state.producer
    #await producer.send_and_wait(topic, send_event)
    asyncio.create_task(producer.send(topic, value=event))