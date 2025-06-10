import uuid
from datetime import datetime, UTC
from typing import Any,Dict

from sqlalchemy import String, UUID, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import TIMESTAMP
from app.core.database import Base


class EventBase(Base):
    __tablename__ = 'events'

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    event_datetime: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, index=True)
    event_name: Mapped[str] = mapped_column(String(256), nullable=True)
    product_id: Mapped[str] = mapped_column(String(64), nullable=False)
    event_json: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


