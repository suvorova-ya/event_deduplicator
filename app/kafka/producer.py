from aiokafka import AIOKafkaProducer
from app.api.schemas import EventCreate


async def send_to_kafka(event: EventCreate):
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    topic = 'products_events'

    await producer.start()
    send_event = event.model_dump_json().encode('utf-8')
    try:
        await producer.send_and_wait(topic,send_event)
    finally:
        await producer.stop()