FROM python:3.13-slim

WORKDIR /opt/app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev bash \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY db/requirements.txt ./requirements.txt
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && rm -rf /root/.cache/pip

# Copy repo (needed for alembic.ini + db/migrations)
COPY . .

# Ensure wait-for-it.sh is executable
COPY scripts/wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN apt-get update && apt-get install -y dos2unix \
    && dos2unix /usr/local/bin/wait-for-it.sh \
    && chmod +x /usr/local/bin/wait-for-it.sh \
    && apt-get purge -y dos2unix && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH=/opt/app

# Default command (overridden in docker-compose)
CMD ["alembic", "upgrade", "head"]
