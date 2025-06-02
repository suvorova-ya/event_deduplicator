FROM python:3.12-slim

# Установим Poetry
RUN pip install --no-cache-dir poetry


WORKDIR /code


COPY pyproject.toml poetry.lock* /code/

# Установим зависимости (без виртуального окружения внутри контейнера)
RUN poetry config virtualenvs.create false \
    && poetry install --only main --only dev --no-root --no-interaction --no-ansi



COPY . /code

EXPOSE 8000

# Команда запуска приложения
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
