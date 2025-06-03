from aiokafka import AIOKafkaConsumer
import asyncio
from app.api.schemas import EventCreate
from app.deduplicator.deduplicator import Deduplicator
from aiokafka.errors import GroupCoordinatorNotAvailableError
import backoff


deduplicator = Deduplicator()

@backoff.on_exception(
    backoff.expo,
    GroupCoordinatorNotAvailableError,
    max_tries=10,
    max_time=60
)


async def consume():
    await deduplicator.init_bloom_filter()
    print("🔁 Consumer стартует и подписывается на Kafka...")
    consumer = AIOKafkaConsumer(
        'products_events',
        bootstrap_servers='kafka:9092',
        auto_offset_reset="earliest",
        group_id='products_consumer_group',
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        max_poll_interval_ms=300000,
        enable_auto_commit=False
    )

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



async def start_consumer():
    while True:
        try:
            await consume()
        except Exception as e:
            print(f"Consumer crashed, restarting... Error: {e}")
            await asyncio.sleep(5)  # Подождать перед перезапуском
