from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import window


def flatten_cdr(df: DataFrame) -> DataFrame:
    return df.select(
        col("call_id"),
        col("caller.id").alias("caller_id"),
        col("caller.location").alias("caller_location"),
        col("callee.id").alias("callee_id"),
        col("callee.location").alias("callee_location"),
        col("call_details.start_time").alias("start_time"),
        col("call_details.end_time").alias("end_time"),
        col("call_details.duration_sec").alias("duration_sec"),
        col("call_details.bytes_used").alias("bytes_used"),
        col("call_details.service_type").alias("service_type"),
        col("network.cell_id").alias("cell_id"),
        col("network.quality").alias("quality"),
        col("metadata.generated_at").alias("generated_at"),
        col("metadata.source").alias("source"),
    )


def aggregate_cdr(df: DataFrame, cfg) -> DataFrame:
    watermarked = df.withWatermark(
        "start_time", f"{cfg.spark.watermark_minutes} minutes"
    )
    return (
        watermarked.groupBy(
            col("caller_id"),
            window(col("start_time"), f"{cfg.spark.window_minutes} minutes"),
        )
        .agg(
            count("*").alias("call_count"),
            _sum("duration_sec").alias("total_duration_s"),
            avg("duration_sec").alias("avg_duration_s"),
            _sum("bytes_used").alias("total_bytes"),
        )
        .select(
            "caller_id",
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "call_count",
            "total_duration_s",
            "avg_duration_s",
            "total_bytes",
        )
    )
