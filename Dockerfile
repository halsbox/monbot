# syntax=docker/dockerfile:1

ARG PY_VERSION=3.11
FROM python:${PY_VERSION}-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# System deps: fonts for skia, tzdata for ZoneInfo, CA certs for HTTPS
RUN apt-get update && apt-get install -y --no-install-recommends \
    libegl1 libgl1 libgles2 \
    libx11-6 libxext6 libxrender1 libxcb1 libxi6 libxrandr2 libxfixes3 libdrm2 \
    libfontconfig1 libfreetype6 fonts-dejavu-core tzdata ca-certificates gosu \
  && rm -rf /var/lib/apt/lists/*

# Create non-root user and data dirs
ARG APP_USER=app
ARG APP_UID=10001
RUN useradd -u ${APP_UID} -m ${APP_USER}
# Make APP_* visible to entrypoint at runtime
ENV APP_USER=${APP_USER} APP_UID=${APP_UID}

# Copy entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /app

# Copy only dependency files first for better layer caching
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy source
# repo layout: /app/monbot/<package>
COPY monbot /app/monbot

# Default data/cache locations; can be overridden at runtime
ENV MONBOT_BASE_DIR=/data \
    MONBOT_CACHE_DIR=/cache \
    MONBOT_REPORTS_DIR=/reports \
    MONBOT_DB_PATH=/data/monbot.db

# Prepare mount points
RUN mkdir -p /data /cache /reports && chown -R ${APP_USER}:${APP_USER} /data /cache /reports /app

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "-m", "monbot.bot"]
