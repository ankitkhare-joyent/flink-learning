# One Dockerfile, one image per Python deployable: --build-arg GROUP=ingest | mcp | agent
# Production pattern for Python + uv: lockfile-driven, non-root, no build tools in the final image.
FROM python:3.13-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.12.22 /uv /usr/local/bin/uv
ARG GROUP=ingest
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /src
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-default-groups --group ${GROUP}
# DuckDB downloads extensions on first use; bake them in so the container needs no internet at runtime.
RUN if /opt/venv/bin/python -c "import duckdb" 2>/dev/null; then \
      /opt/venv/bin/python -c "import duckdb; c=duckdb.connect(); [c.install_extension(e) for e in ('iceberg','httpfs','avro')]"; \
      cp -r /root/.duckdb /opt/duckdb-home; else mkdir -p /opt/duckdb-home; fi

FROM python:3.13-slim
RUN useradd --create-home --uid 10001 app
COPY --from=build /opt/venv /opt/venv
COPY --from=build --chown=app /opt/duckdb-home /home/app/.duckdb
ENV PATH=/opt/venv/bin:$PATH PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY schemas ./schemas
COPY services ./services
COPY scripts ./scripts
COPY stack/connect/cmdb-source.json ./stack/connect/cmdb-source.json
USER app
