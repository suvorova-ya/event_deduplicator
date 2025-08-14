# tests/locustfile.py
import json, random, time, uuid
from pathlib import Path
from locust import FastHttpUser, task, constant

# ── данные для событий ─────────────────────────────────────────────────────────
BASE = Path(__file__).parent
EVENTS = json.loads((BASE / "results.json").read_text())

def make_event(dup_source: dict | None) -> tuple[dict, dict]:
    """
    Возвращает (payload, last).
    В 30% случаев вернем прошлое событие как дубликат.
    """
    if dup_source and random.random() < 0.30:
        return dup_source, dup_source
    base = random.choice(EVENTS).copy()
    base["event_datetime"] = int(time.time() * 1000)
    base["request_id"] = str(uuid.uuid4())
    return base, base

# ── пользователь ───────────────────────────────────────────────────────────────
class ProductUser(FastHttpUser):
    host = "http://fastapi:8000"     # можно переопределить --host в cli
    wait_time = constant(0)          # максимально возможный RPS с учетом задержек

    _last = None

    @task
    def send_event(self):
        payload, self._last = make_event(self._last)
        with self.client.post("/event", json=payload, name="/event", catch_response=True) as r:
            if r.status_code >= 400:
                r.failure(f"{r.status_code} {r.text[:100]}")


