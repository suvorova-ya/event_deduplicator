from collections import deque
from tabnanny import check

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
        """Параллельная обработка с гарантией уникальности"""
        # 1. Сначала проверяем все сообщения на уникальность
        unique_events = await self._filter_unique_events(msgs)

        if unique_events:
            async with self.semaphore:
                try:
                    await self.deduplicator.save_db(values_list=unique_events)
                    logger.info(f"Сохранено {len(unique_events)} событий в БД")
                except Exception as e:
                    logger.error(f"Ошибка при сохранении батча: {e}")

    async def _filter_unique_events(self, msgs):
        """Фильтрация дубликатов через Redis (параллельно)"""
        tasks = []
        for msg in msgs:
            try:
                event = EventCreate.model_validate_json(msg.value.decode('utf-8'))
                logger.info(f" Получено событие: {event.event_name=} {event.client_id=}")
                tasks.append(self._check_and_prepare(event))
            except Exception as e:
                logger.error(f"Ошибка парсинга: {e}")
        results = await asyncio.gather(*tasks, return_exceptions=True)

        unique = []
        for res in results:
            if isinstance(res, Exception):
                # логируем с exc_info, чтобы caplog увидел исключение
                logger.exception("Ошибка при проверке уникальности через Redis")
                continue
            if res is not None:
                unique.append(res)
        return unique


    async def _check_and_prepare(self, event):
        """Атомарная проверка + подготовка данных"""
        try:
            is_unique = await self.deduplicator.check_redis(event.model_dump())
        except Exception:
            # продублируем логирование на уровне элемента
            logger.exception("Сбой check_redis для client_id=%s, event_name=%s",
                             event.client_id, event.event_name)
            return None

        if is_unique:
            return {
                "client_id": event.client_id,
                "product_id": event.product_id,
                "event_datetime": event.event_datetime,
                "event_name": event.event_name,
                "event_json": event.model_dump(mode="json"),
            }
        return None



async def consume(consumer: AIOKafkaConsumer, deduplicator):
    processor = MessageProcessor(deduplicator)
    batch = []

    async for msg in consumer:
        batch.append(msg)
        if len(batch) >= 50:
            try:
                await processor.process_batch(batch)
                await consumer.commit({
                    TopicPartition(m.topic, m.partition): OffsetAndMetadata(m.offset + 1, '')
                    for m in batch
                })
                batch.clear()
            except Exception as e:
                logger.error(f"Ошибка при обработке или коммите батча: {e}")

    if batch:
        try:
            await processor.process_batch(batch)
            await consumer.commit()
        except Exception as e:
            logger.error(f"Ошибка при финальном коммите:{e}")
            raise





