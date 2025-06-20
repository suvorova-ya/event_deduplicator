from app.logging_config import logger, perf_logger
import hashlib
import json
import os
import time

import redis.asyncio as aioredis
from sqlalchemy.exc import SQLAlchemyError

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import connection
from app.core.models import EventBase


DEFAULT_TTL = 60 * 60 * 24 * 7  # 7 дней

class Deduplicator:
    """
     Класс Deduplicator отвечает за проверку уникальности событий
     с использованием Redis и встроенного Redis Bloom Filter.

     Логика работы:
       1. Быстрая проверка хеша события в Redis (SET).
       2. Проверка через Bloom Filter (команда BF.EXISTS).
       3. Если событие уникальное:
          - Добавляет хеш в Redis с TTL (время жизни) 7 дней.
          - Добавляет хеш в Bloom Filter.
          - Увеличивает метрику уникальных событий для Prometheus.
          - Сохраняет событие в базу данных PostgreSQL для последующей аналитики.

     Используется в Kafka consumer для потоковой обработки событий в реальном времени
     """
    def __init__(self,ttl:int=DEFAULT_TTL,bloom_name:str="dedup_filter"):
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.redis = aioredis.from_url(redis_url, decode_responses=True)
        self.ttl = ttl
        self.bloom_name = bloom_name


    model = EventBase


    def compute_hash(self,event:dict):
        fields = {
            "client_id": event.get("client_id"),
            "event_datetime":(event.get("event_datetime")[:19]
                               if isinstance(event.get("event_datetime"), str)
                               else str(event.get("event_datetime"))[:19]
                               ),
            "event_name": event.get("event_name"),
            "product_id": event.get("product_id"),
            "sid": event.get("sid","-"),
            "r": event.get("r"),
            "ts": event.get("ts")
        }
        data = json.dumps(fields)

        return hashlib.sha256(data.encode("utf-8")).hexdigest()

    async def init_bloom_filter(self):
       exists = await self.redis.exists(self.bloom_name)
       if not exists:
           await self.redis.bf().create(self.bloom_name,0.01,10000)

    async def check_redis(self, event: dict):
        start = time.perf_counter()
        hash_value = self.compute_hash(event)
        if await self.redis.exists(hash_value):
            current_ttl = await self.redis.ttl(hash_value)
            logger.info(f"Хэш:  {hash_value} для события {event.get('event_name')} есть в памяти, ttl:{current_ttl}")
            perf_logger.info(f"⏱️ check_redis занял {time.perf_counter() - start:.3f} сек")
            return False

        if await self.redis.bf().exists(self.bloom_name, hash_value):
            logger.info(f"Хэш:  {hash_value} для события {event.get('event_name')} есть в памяти bloom_filter")
            return False

        await self.redis.setex(hash_value, self.ttl, event.get('r'))
        logger.info(f"Добавляем в Bloom-фильтр: {hash_value}")
        await self.redis.bf().add(self.bloom_name, hash_value)
        perf_logger.info(
            f"Уникальное событие, добавлено в Redis и Bloom: {hash_value} по времени заняло: {time.perf_counter() - start:.3f}")
        return True

    @connection
    async def save_db(self, session: AsyncSession, **values):
        start = time.perf_counter()
        new_instance = self.model(**values)
        session.add(new_instance)

        try:
            await session.commit()
        except SQLAlchemyError as e:
            await session.rollback()
            raise e
        finally:
            perf_logger.info(f"💾 save_db занял {time.perf_counter() - start:.3f} сек")
        return new_instance

