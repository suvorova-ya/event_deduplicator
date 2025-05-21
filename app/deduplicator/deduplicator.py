import hashlib
import json

from sqlalchemy.exc import SQLAlchemyError

from sqlalchemy.ext.asyncio import AsyncSession


from app.core.database import connection
from app.core.models import EventBase

class Deduplicator:
    model = EventBase
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
    def compute_hash(self,event:dict):
        fields = {
            "client_id": event.get("client_id"),
            "event_datetime": event.get("event_datetime")[:19],
            "event_name": event.get("event_name"),
            "product_id": event.get("product_id"),
            "sid": event.get("sid","-"),
            "r": event.get("r"),
            "ts": event.get("ts")
        }
        data = json.dumps(fields)

        return hashlib.sha256(data.encode("utf-8")).hexdigest()

    @connection
    async def save_db(self, session: AsyncSession,**values):
        new_instance = self.model(**values)
        session.add(new_instance)
        try:
            await session.commit()
        except SQLAlchemyError as e:
            await session.rollback()
            raise e
        return new_instance

