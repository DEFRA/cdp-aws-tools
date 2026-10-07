FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.8.15 /uv /bin/uv

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy

# entrypoint.sh uploads the audit file with curl.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY app /app/app
COPY entrypoint.sh /app/entrypoint.sh

ENV PATH="/app/.venv/bin:$PATH"

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
