import pytest
from unittest.mock import AsyncMock, patch
from app.kafka.consumer import MessageProcessor
from app.api.schemas import EventCreate



class FakeKafkaMessage:
    def __init__(self, value, offset=0,topic = 'products_events', partition = 0):
        self.value = value.encode('utf-8')
        self.offset = offset
        self.topic = topic
        self.partition = partition



@pytest.mark.asyncio
async def test_process_batch_saves_only_unique(mocker):
    dedup = AsyncMock()
    dedup.check_redis = AsyncMock(side_effect=[True,False,True])
    dedup.save_db = AsyncMock()

    processor = MessageProcessor(dedup, max_concurrent=10)

    valid_event = {
        "client_id": "c1",
        "product_id": "p1",
        "event_datetime": "2025-07-09T12:00:00Z",
        "event_name": "click",
        "sid": "sid_231",
        "r": "r_231",
        "ts": "1745745661138",
        "event_json": {"client_id": "c1", "product_id": "p1"}
    }
    # Готовим тестовые сообщения
    msgs = [
        FakeKafkaMessage(value=EventCreate(**valid_event).model_dump_json(), offset=i)
        for i in range(3)
    ]

    await processor.process_batch(msgs)

    # Проверяем что save_db вызван только для уникальных
    dedup.save_db.assert_awaited_once()
    args, kwargs = dedup.save_db.await_args
    assert len(kwargs["values_list"]) == 2  # Должно сохранить 2 из 3


@pytest.mark.asyncio
async def test_process_batch_saves_invalid_json(caplog):
    dedup = AsyncMock()
    processor = MessageProcessor(dedup, max_concurrent=10)

    invalid_msg = FakeKafkaMessage(value="{not-a-json", offset=10)

    await processor.process_batch([invalid_msg])

    assert 'Ошибка парсинга' in caplog.text

    dedup.save_db.assert_not_called()


@pytest.mark.asyncio
async def test_redis_check_failure_handled(caplog):
    dedup = AsyncMock()

    # Настраиваем mock для check_redis, чтобы он вызывал исключение
    dedup.check_redis = AsyncMock(side_effect=Exception("Redis timeout"))
    dedup.save_db = AsyncMock()

    processor = MessageProcessor(dedup)
    valid_msg = FakeKafkaMessage(
        value=EventCreate(
            client_id="c1",
            product_id="p1",
            event_datetime="2025-01-01T00:00:00Z",
            event_name="view",
            sid="test_sid",
            r="test_r",
            ts="1234567890"
        ).model_dump_json()
    )

    await processor.process_batch([valid_msg])

    # Проверяем что save_db не вызывался
    dedup.save_db.assert_not_called()

    # Проверяем что ошибка была залогирована
    assert any("Redis timeout" in str(record.exc_info[1]) for record in caplog.records if record.exc_info)