import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


# === Формат логов ===
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")


# === Основной логгер ===
log_file = os.path.join(LOG_DIR, "app.log")
file_handler = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3)
file_handler.setFormatter(formatter)

logger = logging.getLogger("product_logger")
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())  # для stdout

# === Логгер для performance ===
perf_file = os.path.join(LOG_DIR, "performance.log")
perf_file_handler = RotatingFileHandler(perf_file, maxBytes=5_000_000, backupCount=2)
perf_file_handler.setFormatter(formatter)

perf_logger = logging.getLogger("perf_logger")
perf_logger.setLevel(logging.INFO)
perf_logger.addHandler(perf_file_handler)
perf_logger.propagate = False
# perf_logger.addHandler(logging.StreamHandler()) # для stdout
