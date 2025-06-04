"""
Locust-сценарий: поднимаем нагрузку до 500 RPS, 30 % запросов – дубликаты.

Запуск в headless-режиме:
docker compose run --rm locust -f tests/locustfile.py --headless \
    -u 500 -r 50 -t 5m --host http://fastapi_app:8000
"""

import itertools
import json
import random
import time
import uuid
from pathlib import Path

from locust import HttpUser, LoadTestShape, between, task


import logging
from datetime import datetime

BASE = Path(__file__).parent



log_file = BASE.parent / "logs" / f"locust_{datetime.now():%Y-%m-%d_%H-%M-%S}.log"
log_file.parent.mkdir(parents=True, exist_ok=True)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


BASE = Path(__file__).parent
DATA = (BASE / "test_events_350.json").read_text(encoding="utf-8")
events = json.loads(DATA)


def iter_events():
    """Бесконечный поток событий, каждое третье – дубликат предыдущего."""
    source = itertools.cycle(events)  # ← циклим  по списку словарей
    last = None
    for item in source:
        if random.random() < 0.3 and last:
            yield last
        else:
            ev = item.copy()
            ev["event_datetime"] = int(time.time() * 1000)
            ev["request_id"] = str(uuid.uuid4())
            last = ev
            yield ev


_EVENTS = iter_events()


class ProductUser(HttpUser):
    host = "http://fastapi:8000"
    wait_time = between(0.01, 0.05)

    @task
    def send_event(self):
        response = self.client.post("/event", json=next(_EVENTS), name="/event")
        if response.status_code >= 400:
            logger.warning(f"❌ Ошибка {response.status_code}: {response.text}")


# ───── профиль нагрузки: 0→500 rps, держим, затем спад ────────────────────────
class Stages(LoadTestShape):
    stages = [
        {"duration": 60, "users": 500, "spawn_rate": 50},
        {"duration": 300, "users": 500, "spawn_rate": 1},
        {"duration": 360, "users": 0, "spawn_rate": 50},
    ]

    def tick(self):
        run = self.get_run_time()
        total = 0
        for s in self.stages:
            total += s["duration"]
            if run < total:
                return s["users"], s["spawn_rate"]
        return None


