import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader

from common.config.bridge.models import GeneratorSettings, KafkaSettings, ProducerTuning

# Paths
PROJECT_ROOT = Path(__file__).resolve().parents[3]  # repo root
BASE_DIR = PROJECT_ROOT / "common"
TEMPLATE_DIR = BASE_DIR / "config" / "templates"


def render_template(template_name: str, context: dict, out_path: Path):
    """Render a Jinja2 template with the given context into a file."""
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
    template = env.get_template(template_name)
    out_path.write_text(template.render(env=os.environ, **context))


def require_env(key: str) -> str:
    """Fetch required env var or fail fast."""
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {key}")
    return val


def build_generator_config(out_dir: Path):
    # Always load .env from project root
    load_dotenv(PROJECT_ROOT / ".env")

    settings = GeneratorSettings(
        rate_per_sec=int(os.getenv("GENERATOR_RATE_PER_SEC", 10)),
        seed=int(os.getenv("GENERATOR_SEED", 42)),
    )

    context = {
        "generator": {
            "rate_per_sec": settings.rate_per_sec,
            "seed": settings.seed,
            "producer_url": os.getenv(
                "PRODUCER_URL", "http://producer:8080/api/v1/publish"
            ),
        }
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    render_template("generator.yml.j2", context, out_dir / "generator.yml")
    render_template("logging.json.j2", {}, out_dir / "logging.json")


def build_producer_config(out_dir: Path):
    # FIX: use project root instead of BASE_DIR
    load_dotenv(PROJECT_ROOT / ".env")

    kafka_settings = KafkaSettings(
        bootstrap_servers=require_env("KAFKA_BROKER"),
        topic_raw=require_env("KAFKA_TOPIC_RAW"),
        topic_flat=require_env("KAFKA_TOPIC_FLAT"),
        topic_error=require_env("KAFKA_TOPIC_ERROR"),
        security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
    )

    tuning = ProducerTuning(
        linger_ms=int(os.getenv("PRODUCER_LINGER_MS", 5)),
        batch_bytes=int(os.getenv("PRODUCER_BATCH_BYTES", 32768)),
        compression=os.getenv("PRODUCER_COMPRESSION", "lz4"),
        retries=int(os.getenv("PRODUCER_RETRIES", 10)),
        enable_idempotence=(
            os.getenv("PRODUCER_ENABLE_IDEMPOTENCE", "true").lower()
            in ("true", "1", "yes")
        ),
        max_in_flight=int(os.getenv("PRODUCER_MAX_IN_FLIGHT", 5)),
    )

    context = {
        "KAFKA_BROKER": kafka_settings.bootstrap_servers,
        "KAFKA_TOPIC_RAW": kafka_settings.topic_raw,
        "KAFKA_TOPIC_FLAT": kafka_settings.topic_flat,
        "KAFKA_TOPIC_ERROR": kafka_settings.topic_error,
        "KAFKA_SECURITY_PROTOCOL": kafka_settings.security_protocol,
        "PRODUCER_LINGER_MS": tuning.linger_ms,
        "PRODUCER_BATCH_BYTES": tuning.batch_bytes,
        "PRODUCER_COMPRESSION": tuning.compression,
        "PRODUCER_RETRIES": tuning.retries,
        "PRODUCER_ENABLE_IDEMPOTENCE": tuning.enable_idempotence,
        "PRODUCER_MAX_IN_FLIGHT": tuning.max_in_flight,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    render_template("producer.yml.j2", context, out_dir / "producer.yml")
    render_template("logging.json.j2", {}, out_dir / "logging.json")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--service", required=True, choices=["generator", "producer", "spark"]
    )
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    out_dir = Path(args.out)

    if args.service == "generator":
        build_generator_config(out_dir)
    elif args.service == "producer":
        build_producer_config(out_dir)
    else:
        raise NotImplementedError(f"Service {args.service} not yet supported")


if __name__ == "__main__":
    main()
