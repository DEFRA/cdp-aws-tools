FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml /app/pyproject.toml
COPY app /app/app
COPY entrypoint.sh /app/entrypoint.sh

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
