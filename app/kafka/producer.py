from app.api.schemas import EventCreate
from fastapi import Request


async def send_to_kafka(request: Request, event: EventCreate):
    topic = 'products_events'
    send_event = event.model_dump_json().encode('utf-8')
    print(f"Отправка события в Kafka: {send_event}")

    producer = request.app.state.producer
    await producer.send_and_wait(topic, send_event)