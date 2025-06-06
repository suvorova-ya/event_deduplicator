from fastapi import APIRouter
from starlette.responses import JSONResponse
from fastapi import Request
from app.api.schemas import EventCreate
from app.kafka.producer import send_to_kafka

router = APIRouter(tags=["events"])


@router.post("/event",summary="Отправка продуктового события в Kafka",
             description="""Принимает событие, сериализует его и отправляет в Kafka для
             дальнейшей обработки и дедупликации""")
async def receive_event (event: EventCreate, request: Request):
    await send_to_kafka(request,event)
    return JSONResponse(content={"status": "ok"}, status_code=200)






