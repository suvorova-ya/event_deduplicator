from collections import deque

from aiokafka import AIOKafkaConsumer, TopicPartition, OffsetAndMetadata
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


class MessageProcessor:
    def __init__(self,deduplicator, max_concurrent=100):
        self.deduplicator = deduplicator
        self.semaphore = asyncio.Semaphore(max_concurrent)


    async def process_batch(self,msgs):
        """Параллельная обработка батча сообщений"""
        values_list = []
        tasks =[]
        for msg in msgs:
            task = asyncio.create_task(self._prepare_event(msg, values_list))
            tasks.append(task)
        results = await asyncio.gather(*tasks,return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                logger.error(f"Ошибка в батче: {r}")

        if values_list:
            async with self.semaphore:
                try:
                    await self.deduplicator.save_db(values_list=values_list)
                    logger.info(f"Сохранено {len(values_list)} событий в БД")
                except Exception as e:
                    logger.error(f"Ошибка при сохранении батча: {e}")

    async def _prepare_event(self,msg,values_list: list):
        try:
            event = EventCreate.model_validate_json(msg.value.decode('utf-8'))
            logger.info(f" Получено событие: {event.event_name=} {event.client_id=}")
            if await self.deduplicator.check_redis(event.model_dump()):
                values_list.append({
                "client_id" : event.client_id,
                "product_id" :event.product_id,
                "event_datetime" : event.event_datetime,
                "event_name" :event.event_name,
                "event_json" : event.model_dump(mode="json")
                })
                logger.info(f"Событие сохранено в БД: {event.client_id}")
        except Exception as e:
            logger.error(f"Ошибка обработки: {e}")


async def consume(consumer: AIOKafkaConsumer, deduplicator):
    processor = MessageProcessor(deduplicator)
    batch = []

    async for msg in consumer:
        batch.append(msg)
        if len(batch) >= 50:
                await processor.process_batch(batch)
                await consumer.commit({
                    TopicPartition(m.topic, m.partition): OffsetAndMetadata(m.offset + 1, '')
                    for m in batch
                })
                batch.clear()

    if batch:
        await processor.process_batch(batch)
        await consumer.commit()





