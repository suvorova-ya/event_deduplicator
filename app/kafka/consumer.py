from aiokafka import AIOKafkaConsumer
import asyncio
from app.api.schemas import EventCreate
from app.deduplicator.deduplicator import Deduplicator

deduplicator = Deduplicator()




async def consume():
    await deduplicator.init_bloom_filter()
    consumer = AIOKafkaConsumer(
        'products_events',
        bootstrap_servers='localhost:9092',
        auto_offset_reset="earliest",
        group_id='products_consumer_group')

    await consumer.start()
    try:
        async for event in consumer:

            event_str = event.value.decode('utf-8')

            event = EventCreate.model_validate_json(event_str)

            print(f" Получено событие: {event.event_name=} {event.client_id=}")
            if await deduplicator.check_redis(event.model_dump()):

                await deduplicator.save_db(
                    client_id=event.client_id,
                    product_id=event.product_id,
                    event_datetime=event.event_datetime,
                    event_name=event.event_name,
                    event_json=event.model_dump(mode="json")  # dict для JSONB
                )
    finally:
        await consumer.stop()

if __name__ == '__main__':
    asyncio.run(consume())
