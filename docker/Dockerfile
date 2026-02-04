# Multi-stage Dockerfile for Theme Parks Data Pipeline
# Following official uv best practices: https://docs.astral.sh/uv/guides/integration/docker/

# =============================================================================
# Stage 1: Builder
# =============================================================================
FROM python:3.12-slim-bookworm AS builder

# Install uv by copying from official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Change working directory
WORKDIR /app

# Enable bytecode compilation for faster startup
ENV UV_COMPILE_BYTECODE=1

# Use copy mode to improve caching
ENV UV_LINK_MODE=copy

# Install dependencies (separate layer for caching)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

# Copy application code
COPY . /app

# Install the project in non-editable mode
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# =============================================================================
# Stage 2: Runtime
# =============================================================================
FROM python:3.12-slim-bookworm

LABEL maintainer="data-engineering@example.com"
LABEL description="Theme Parks Data Pipeline - Production Runtime"
LABEL version="2.0.0"

# Create non-root user for security
RUN groupadd -r themeparks && useradd -r -g themeparks -u 1000 themeparks

WORKDIR /app

# Copy the virtual environment from builder (no source code needed with --no-editable)
COPY --from=builder --chown=themeparks:themeparks /app/.venv /app/.venv

# Copy application code
COPY --chown=themeparks:themeparks themeparks/ ./themeparks/
COPY --chown=themeparks:themeparks extractors/ ./extractors/

# Create directory for local file outputs (if used)
RUN mkdir -p /app/data && chown -R themeparks:themeparks /app

# Switch to non-root user
USER themeparks

# Place the virtual environment at the front of PATH
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Health check (can be overridden in k8s)
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import themeparks; print('healthy')" || exit 1

# Default entrypoint
ENTRYPOINT ["python", "-m", "themeparks"]

# Default command (show help)
CMD ["--help"]
