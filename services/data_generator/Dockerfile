FROM python:3.13-slim

WORKDIR /opt/app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY services/data_generator/requirements.txt ./requirements.txt
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy the whole repo (keep directory structure)
COPY . .

# Environment & runtime setup
ENV GENERATOR_CONFIG=/opt/app/config/generator.yml
ENV PYTHONPATH=/opt/app
EXPOSE 8000

VOLUME ["/opt/app/config"]

# Module-qualified entrypoint builds config and runs generator
ENTRYPOINT ["sh", "-c", "python -m common.config.bridge.build_config --service generator --out /opt/app/config && python -m services.data_generator.main"]
