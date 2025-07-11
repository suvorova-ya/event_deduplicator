import pytest
from unittest.mock import AsyncMock
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
    msg_1 = FakeKafkaMessage(value=EventCreate(**valid_event).model_dump_json(),offset=1)
    msg_2 = FakeKafkaMessage(value=EventCreate(**valid_event).model_dump_json(), offset=2)
    msg_3 = FakeKafkaMessage(value=EventCreate(**valid_event).model_dump_json(), offset=3)

    batch = [msg_1,msg_2,msg_3]

    await processor.process_batch(batch)

    assert dedup.check_redis.await_count == 3
    dedup.save_db.assert_awaited_once()
    args, kwargs = dedup.save_db.await_args
    values_list = kwargs["values_list"]
    assert len(values_list) == 2

@pytest.mark.asyncio
async def test_process_batch_saves_invalid_json(caplog):
    dedup = AsyncMock()
    dedup.check_redis = AsyncMock(return_value=True)
    dedup.save_db = AsyncMock()

    processor = MessageProcessor(dedup, max_concurrent=10)
    invalid_msg = FakeKafkaMessage(value="{not-a-json",offset=10)


    await processor.process_batch([invalid_msg])

    dedup.save_db.assert_not_called()
    assert any('Ошибка обработки' in r.message for r in caplog.records)
