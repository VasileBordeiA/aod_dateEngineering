FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.11.13 /uv /uvx /bin/
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1 TZ=UTC PATH="/app/.venv/bin:$PATH"
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev
RUN useradd --system --uid 10001 app
USER app
EXPOSE 8000
# Only Caddy (and this laptop, via 127.0.0.1) can reach :8000, so trusting X-Forwarded-For from anyone is safe (spec 09).
CMD ["uvicorn", "api.main:app_from_env", "--factory", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips=*", "--no-access-log"]
