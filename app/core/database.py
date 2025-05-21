
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs, AsyncSession
from sqlalchemy.orm import DeclarativeBase

from app.core.config import settings

DATABASE_URL = settings.db_url

engine = create_async_engine(DATABASE_URL)
async_session_maker = async_sessionmaker(engine, expire_on_commit=False)


async def get_async_session() -> AsyncSession:
    async with async_session_maker() as session:
        yield session


def connection(method):
    async def wrapper(*args, **kwargs):
        async with async_session_maker() as session:
            try:
                # Явно не открываем транзакции, так как они уже есть в контексте
                return await method(*args, session=session, **kwargs)
            except Exception as e:
                await session.rollback()  # Откатываем сессию при ошибке
                raise e  # Поднимаем исключение дальше
            finally:
                await session.close()  # Закрываем сессию

    return wrapper


class Base(AsyncAttrs, DeclarativeBase):
    __abstract__ = True


