from datetime import datetime

from pydantic import BaseModel, ConfigDict


class EventCreate(BaseModel):
    client_id: str
    event_datetime : datetime
    event_name : str
    product_id : str
    sid : str
    r : str
    ts : str

    model_config = ConfigDict(extra="allow")


