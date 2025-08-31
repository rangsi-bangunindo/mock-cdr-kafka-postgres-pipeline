FROM python:3.13-slim

WORKDIR /opt/app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev bash \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY services/producer/requirements.txt ./requirements.txt
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy the whole repo (keep directory structure)
COPY . .

# Ensure wait-for-it.sh is executable inside the image
COPY scripts/wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN apt-get update && apt-get install -y dos2unix \
    && dos2unix /usr/local/bin/wait-for-it.sh \
    && chmod +x /usr/local/bin/wait-for-it.sh \
    && apt-get purge -y dos2unix && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# Environment & runtime setup
ENV PRODUCER_CONFIG=/opt/app/config/producer.yml
ENV PYTHONPATH=/opt/app
EXPOSE 8080

VOLUME ["/opt/app/config"]

# Entrypoint uses wait-for-it.sh to block until Kafka is ready
ENTRYPOINT ["sh", "-c", "wait-for-it.sh kafka:9092 -- python -m common.config.bridge.build_config --service producer --out /opt/app/config && uvicorn services.producer.app:app --host 0.0.0.0 --port 8080"]
