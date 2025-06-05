import logging
from logging.handlers import RotatingFileHandler
import os

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# === Основной логгер ===
log_file = os.path.join(LOG_DIR, "app.log")
file_handler = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
file_handler.setFormatter(formatter)

logger = logging.getLogger("product_logger")
logger.setLevel(logging.WARNING)
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
