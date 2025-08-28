import asyncio
import json
import logging
import logging.config
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

import httpx
import jsonschema
import yaml
from dotenv import load_dotenv

from common.config.bridge.models import GeneratorSettings

# Load environment variables for bridge
load_dotenv()

# Read generated config (bridge has already rendered it)
CONFIG_PATH = os.getenv(
    "GENERATOR_CONFIG",
    "/opt/app/config/generator.yml",
)
if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(
        f"Missing config file at {CONFIG_PATH}. " "Did you run build_config?"
    )

with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

settings = GeneratorSettings(**config["generator"])

# Logging
LOGGING_CONFIG_PATH = os.getenv(
    "LOGGING_CONFIG",
    "/opt/app/config/logging.json",
)
with open(LOGGING_CONFIG_PATH) as f:
    logging_config = json.load(f)
logging.config.dictConfig(logging_config)
logger = logging.getLogger("data_generator")


# Load schema
SCHEMA_PATH = os.getenv(
    "CDR_SCHEMA",
    os.path.join(
        os.path.dirname(__file__),
        "../../common/schemas/cdr_schema.json",
    ),
)
with open(SCHEMA_PATH) as f:
    CDR_SCHEMA = json.load(f)

# HTTP client to Producer (e.g. http://producer:8080/api/v1/publish)
PRODUCER_URL = config["producer"]["url"]
client = httpx.AsyncClient(timeout=5.0)


def random_cdr() -> dict:
    """Generate one CDR with deterministic randomness."""
    call_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)
    duration = random.randint(30, 600)  # 30s to 10min
    end_time = start_time + timedelta(seconds=duration)

    cdr = {
        "call_id": call_id,
        "caller": {
            "id": f"user{random.randint(1, 100)}",
            "location": random.choice(["Jakarta", "Bandung", "Surabaya"]),
        },
        "callee": {
            "id": f"user{random.randint(101, 200)}",
            "location": random.choice(["Jakarta", "Bandung", "Surabaya"]),
        },
        "call_details": {
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_sec": duration,
            "bytes_used": random.randint(10_000, 5_000_000),
            "service_type": random.choice(["voice", "sms", "data"]),
        },
        "network": {
            "cell_id": f"CID-{random.randint(1, 10)}",
            "quality": random.choice(["good", "fair", "poor"]),
        },
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": "simulator",
        },
    }

    return cdr


async def send_batch(batch_size: int):
    records = []
    for _ in range(batch_size):
        record = random_cdr()
        try:
            jsonschema.validate(instance=record, schema=CDR_SCHEMA)
            records.append(record)
        except jsonschema.ValidationError as e:
            logger.error("Invalid CDR generated", extra={"error": str(e)})

    if records:
        try:
            resp = await client.post(PRODUCER_URL, json={"records": records})
            resp.raise_for_status()
            logger.info(
                "Sent batch",
                extra={
                    "size": len(records),
                    "status": resp.status_code,
                },
            )
        except Exception as e:
            logger.error("Failed to send batch", extra={"error": str(e)})


async def main():
    random.seed(settings.seed)
    rate = settings.rate_per_sec
    batch_size = min(rate, 100)

    while True:
        await send_batch(batch_size)
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
