from fastapi import APIRouter
from typing import List, Union
from fastapi import Request
from app.api.schemas import EventCreate
from app.kafka.producer import send_to_kafka

router = APIRouter(tags=["events"])


@router.post("/event",summary="Отправка продуктового события в Kafka",
             description="""Принимает событие, сериализует его и отправляет в Kafka для
             дальнейшей обработки и дедупликации""")
async def receive_event(event: Union[EventCreate, List[EventCreate]], request: Request):
    events = event if isinstance(event, list) else [event]
    await send_to_kafka(request, events)
    return {"status": "ok", "received": len(events)}






