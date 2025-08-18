import asyncio

from app.api.schemas import EventCreate
from fastapi import Request
from app.logging_config import logger



async def send_to_kafka(request: Request, events: list[EventCreate]):
    topic = 'products_events'
    producer = request.app.state.producer

    # Сериализация всех событий в список байтов
    batch_data = [e.model_dump_json().encode('utf-8') for e in events]
    # Отправляем все события асинхронно
    send_tasks = [producer.send(topic, data) for data in batch_data]
    logger.info(f"Отправка события в Kafka: {send_tasks}")
    await asyncio.gather(*send_tasks)

    await producer.flush()
