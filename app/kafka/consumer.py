from aiokafka import AIOKafkaConsumer
import asyncio
from app.api.schemas import EventCreate
from aiokafka.errors import GroupCoordinatorNotAvailableError
import backoff
from app.logging_config import logger



@backoff.on_exception(
    backoff.expo,
    GroupCoordinatorNotAvailableError,
    max_tries=10,
    max_time=60
)


async def consume(consumer: AIOKafkaConsumer, deduplicator):
    try:
        async for msg in consumer:
            try:
                msg_str = msg.value.decode('utf-8')

                event = EventCreate.model_validate_json(msg_str)

                logger.info(f" Получено событие: {event.event_name=} {event.client_id=}")
                is_unique = await deduplicator.check_redis(event.model_dump())
                if is_unique:
                        await deduplicator.save_db(
                            client_id=event.client_id,
                            product_id=event.product_id,
                            event_datetime=event.event_datetime,
                            event_name=event.event_name,
                            event_json=event.model_dump(mode="json")  # dict для JSONB
                        )
                        logger.info(f"Событие сохранено в БД: {event.client_id}")

                await consumer.commit()
            except Exception as exc_inner:
                logger.error(f"Ошибка при обработке сообщения: {exc_inner}")

    except asyncio.CancelledError:
        logger.info("Консьюмер остановлен по запросу")
    except Exception as e:
        logger.error(f"Критическая ошибка консьюмера: {e}")





