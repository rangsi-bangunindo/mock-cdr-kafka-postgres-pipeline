import json
import logging
import os

import jsonschema
from fastapi import FastAPI
from pydantic import BaseModel

from services.producer.config_loader import load_config
from services.producer.producer import KafkaProducerWrapper

logger = logging.getLogger("producer")

# Load schema properly from file
SCHEMA_PATH = os.getenv("CDR_SCHEMA", "/common/schemas/cdr_schema.json")
with open(SCHEMA_PATH) as f:
    CDR_SCHEMA = json.load(f)

# Load config and producer
cfg = load_config()
producer = KafkaProducerWrapper(cfg.kafka, cfg.tuning)

app = FastAPI()


class PublishRequest(BaseModel):
    records: list[dict]


@app.post("/api/v1/publish")
async def publish(payload: PublishRequest):
    success = 0
    failures = 0
    for record in payload.records:
        try:
            jsonschema.validate(instance=record, schema=CDR_SCHEMA)
            key = record.get("caller", {}).get("id") or record.get("call_id")
            producer.produce(
                cfg.kafka.topic_raw,
                key=key,
                value=json.dumps(record),
            )
            success += 1
        except jsonschema.ValidationError as e:
            failures += 1
            error_payload = {"error": str(e), "payload": record}
            producer.produce(
                cfg.kafka.topic_error,
                key=record.get("call_id"),
                value=json.dumps(error_payload),
            )
    return {"success": success, "failures": failures}


@app.get("/healthz")
async def health():
    return {"status": "ok"}
