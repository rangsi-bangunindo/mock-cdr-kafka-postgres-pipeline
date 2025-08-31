import json
import logging
import logging.config
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType

from services.spark_job.config_loader import load_config
from services.spark_job.transform import aggregate_cdr, flatten_cdr


def setup_logging(cfg):
    config_path = cfg.logging.config_path if cfg.logging else None

    # Fallback: default to generated logging.json
    if not config_path:
        candidate = "/opt/app/config/logging.json"
        if os.path.exists(candidate):
            config_path = candidate

    if not config_path or not os.path.exists(config_path):
        raise RuntimeError("Logging config_path not found or missing in configuration")

    with open(config_path) as f:
        config = json.load(f)
    logging.config.dictConfig(config)


def main():
    cfg = load_config()
    setup_logging(cfg)
    logger = logging.getLogger("spark_job")

    spark = (
        SparkSession.builder.appName("cdr-spark-job")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Load schema
    with open("/opt/app/common/schemas/cdr_spark_schema.json") as f:
        schema_dict = json.load(f)
    schema = StructType.fromJson(schema_dict)

    # Read raw Kafka stream
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka.bootstrap_servers)
        .option("subscribe", cfg.kafka.topic_raw)
        .option("startingOffsets", cfg.kafka.starting_offsets)
        .option("maxOffsetsPerTrigger", cfg.kafka.max_offsets_per_trigger)
        .option("failOnDataLoss", str(cfg.spark.fail_on_data_loss).lower())
        .option("kafka.request.timeout.ms", "20000")
        .option("kafka.retry.backoff.ms", "500")
        .load()
    )

    parsed = raw_stream.select(
        col("value").alias("raw_value"),
        from_json(col("value").cast("string"), schema).alias("cdr"),
    )

    # Split good vs error records
    valid = parsed.filter(col("cdr").isNotNull()).select("cdr.*")
    errors = parsed.filter(col("cdr").isNull()).select("raw_value")

    # Keep references to streaming queries to avoid GC
    queries = []

    # Write errors to Kafka (hardened)
    error_stream = (
        errors.selectExpr("CAST(NULL AS STRING) AS key", "raw_value AS value")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka.bootstrap_servers)
        .option("kafka.acks", "all")
        .option("topic", cfg.kafka.topic_error)
        .option("checkpointLocation", f"{cfg.spark.checkpoint_dir}/errors")
        .outputMode("append")
        .start()
    )
    queries.append(error_stream)

    # Flattened output
    flattened = flatten_cdr(valid)

    # Flattened output (hardened)
    flat_stream = (
        flattened.selectExpr(
            "CAST(caller_id AS STRING) AS key",
            "to_json(struct(*)) AS value",
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka.bootstrap_servers)
        .option("kafka.acks", "all")
        .option("topic", cfg.kafka.topic_flat)
        .option("checkpointLocation", f"{cfg.spark.checkpoint_dir}/flattened")
        .outputMode("append")
        .start()
    )
    queries.append(flat_stream)

    # Aggregates → Postgres (use flattened, not raw)
    aggregated = aggregate_cdr(flattened, cfg)

    def write_to_pg(batch_df, batch_id):
        from psycopg2 import connect
        from psycopg2.extras import execute_batch

        rows = [
            (
                row.caller_id,
                row.window_start,
                row.window_end,
                row.call_count,
                row.total_duration_s,
                row.avg_duration_s,
                row.total_bytes,
            )
            for row in batch_df.toLocalIterator()
        ]

        if not rows:
            return

        conn = None
        try:
            conn = connect(
                host=cfg.postgres.host,
                port=cfg.postgres.port,
                dbname=cfg.postgres.database,
                user=cfg.postgres.user,
                password=cfg.postgres.password,
            )
            with conn.cursor() as cur:
                execute_batch(
                    cur,
                    """
                    INSERT INTO cdr_usage_summary
                    (caller_id, window_start, window_end, call_count,
                     total_duration_s, avg_duration_s, total_bytes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (caller_id, window_start, window_end)
                    DO UPDATE SET
                        call_count = EXCLUDED.call_count,
                        total_duration_s = EXCLUDED.total_duration_s,
                        avg_duration_s = EXCLUDED.avg_duration_s,
                        total_bytes = EXCLUDED.total_bytes,
                        last_updated_at = NOW()
                """,
                    rows,
                )
            conn.commit()
        except Exception as e:
            logger.error(f"DB batch write failed: {e}", exc_info=True)
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    agg_query = (
        aggregated.writeStream.outputMode("update")
        .foreachBatch(write_to_pg)
        .option("checkpointLocation", f"{cfg.spark.checkpoint_dir}/aggregates")
        .start()
    )
    queries.append(agg_query)

    # Block until termination
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
