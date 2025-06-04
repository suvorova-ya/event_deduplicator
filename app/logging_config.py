# app/logging_config.py
import logging
from logging.handlers import RotatingFileHandler
import os

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, "app.log")

handler = RotatingFileHandler(log_file, maxBytes=10_000_000, backupCount=3)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        handler,
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("app")
