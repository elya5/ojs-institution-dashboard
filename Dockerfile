# ---------- Stage 1: build the Vue frontend ----------
FROM node:22-slim AS frontend
WORKDIR /build
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# ---------- Stage 2: Python runtime ----------
FROM python:3.14-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY . .
RUN uv sync --frozen --no-dev

COPY --from=frontend /build/dist ./frontend/dist

# HF Spaces runs containers as UID 1000
RUN useradd -m -u 1000 user && chown -R user:user /app
USER user

ENV HF_HOME=/tmp/hf
ENV ARTICLE_CACHE_PATH=/tmp/cache

EXPOSE 7860
CMD ["/app/.venv/bin/uvicorn", "server:app", "--host", "0.0.0.0", "--port", "7860"]
