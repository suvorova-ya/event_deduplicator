import asyncio

from app.deduplicator.deduplicator import Deduplicator

from app.logging_config import logger




async def periodic_cleanup():
    deduplicator = Deduplicator()
    while True:
        logger.info('Удаление событий старше 7 дней')
        await deduplicator.del_old_events()
        await asyncio.sleep(24 * 60 * 60)  # раз в сутки

if __name__ == "__main__":
    asyncio.run(periodic_cleanup())