
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    when,
    trim,
    length,
    sum as spark_sum
)

# --------------------------------------------------
# Configuration
# --------------------------------------------------

HDFS_PATH = (
    "hdfs://namenode:9000/data/bronze/wikipedia/"
    "ingestion_date=*/scraped_text_*.jsonl"
)

SHORT_ARTICLE_THRESHOLD = 100


# --------------------------------------------------
# Bronze Schema
# --------------------------------------------------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)

bronze_schema = StructType([
    StructField("record_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("source_name", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("source_type", StringType(), True),
    StructField("page_id", LongType(), True),
    StructField("revision_id", LongType(), True),
    StructField("revision_timestamp", StringType(), True),
    StructField("language", StringType(), True),
    StructField("dialect", StringType(), True),
    StructField("license", StringType(), True),
    StructField("collection_timestamp", StringType(), True)
])


# --------------------------------------------------
# Data Quality Assessment
# --------------------------------------------------

def main():

    spark = (
        SparkSession.builder
        .appName("EgyptianArabicTTS-BronzeQualityAssessment")
        .getOrCreate()
    )

    print("\n========== BRONZE DATA QUALITY ASSESSMENT ==========\n")

    # 1. Read Bronze data
    df = spark.read.schema(bronze_schema).json(HDFS_PATH)

    # 2. Total records
    total_records = df.count()

    print(f"Total records: {total_records}")

    # 3. Basic quality metrics
    quality_report = df.select(

        # Record ID checks
        count(
            when(col("record_id").isNull(), 1)
        ).alias("null_record_id"),

        count(
            when(
                col("record_id").isNotNull()
                & (trim(col("record_id")) == ""),
                1
            )
        ).alias("empty_record_id"),

        # Title checks
        count(
            when(col("title").isNull(), 1)
        ).alias("null_title"),

        count(
            when(
                col("title").isNotNull()
                & (trim(col("title")) == ""),
                1
            )
        ).alias("empty_title"),

        # Text checks
        count(
            when(col("text").isNull(), 1)
        ).alias("null_text"),

        count(
            when(
                col("text").isNotNull()
                & (trim(col("text")) == ""),
                1
            )
        ).alias("empty_text"),

        # Metadata checks
        count(
            when(
                col("page_id").isNull()
                | (col("page_id") <= 0),
                1
            )
        ).alias("invalid_page_id"),

        count(
            when(
                col("revision_id").isNull()
                | (col("revision_id") <= 0),
                1
            )
        ).alias("invalid_revision_id"),

        # Diagnostic check: short articles
        count(
            when(
                col("text").isNotNull()
                & (length(trim(col("text"))) < SHORT_ARTICLE_THRESHOLD),
                1
            )
        ).alias("short_articles")

    )

    print("\n--- Basic Quality Metrics ---")
    quality_report.show(truncate=False)

    # 4. Duplicate record ID checks
    duplicate_id_groups = (
        df.groupBy("record_id")
        .count()
        .filter(
            col("record_id").isNotNull()
            & (col("count") > 1)
        )
    )

    duplicate_group_count = duplicate_id_groups.count()

    duplicate_rows = (
        duplicate_id_groups
        .select(
            spark_sum(col("count") - 1).alias("duplicate_rows")
        )
        .first()["duplicate_rows"]
    )

    duplicate_rows = duplicate_rows or 0

    print("\n--- Duplicate Record ID Metrics ---")
    print(f"Duplicate ID groups: {duplicate_group_count}")
    print(f"Duplicate rows beyond first occurrence: {duplicate_rows}")

    if duplicate_group_count > 0:
        print("\nDuplicate ID examples:")
        duplicate_id_groups.show(10, truncate=False)

    # 5. Record ID uniqueness summary
    distinct_record_ids = df.select("record_id").distinct().count()

    print("\n--- Record ID Summary ---")
    print(f"Total records: {total_records}")
    print(f"Distinct record IDs: {distinct_record_ids}")

    print("\n========== ASSESSMENT COMPLETED ==========\n")

    spark.stop()


if __name__ == "__main__":
    main()
