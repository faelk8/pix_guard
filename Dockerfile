FROM python:3.12.12-slim-bookworm AS builder

ENV PIP_NO_CACHE_DIR=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=true
WORKDIR /app
RUN pip install --no-cache-dir poetry==2.2.1
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root --with test
COPY src ./src
RUN poetry install --only-root

FROM python:3.12.12-slim-bookworm AS runtime
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH" \
    PIXGUARD_DATA_DIR=/data/pixguard
RUN groupadd --gid 10001 pixguard \
    && useradd --uid 10001 --gid pixguard --no-create-home pixguard \
    && mkdir -p /data/pixguard \
    && chown pixguard:pixguard /data/pixguard
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY --chown=pixguard:pixguard src ./src
COPY --chown=pixguard:pixguard tests ./tests
COPY pyproject.toml ./
USER pixguard
CMD ["pixguard", "idle"]
