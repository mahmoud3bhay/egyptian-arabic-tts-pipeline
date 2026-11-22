
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    regexp_replace,
    length,
    avg,
    count,
    when
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)


# --------------------------------------------------
# Configuration
# --------------------------------------------------

HDFS_PATH = (
    "hdfs://namenode:9000/data/bronze/wikipedia/"
    "ingestion_date=*/scraped_text_*.jsonl"
)


# --------------------------------------------------
# Bronze Schema
# --------------------------------------------------

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
# Text Cleaning Function
# --------------------------------------------------

def normalize_text(text_column):

    # Replace tabs, newlines, and carriage returns with spaces.
    cleaned = regexp_replace(
        text_column,
        r"[\t\n\r]+",
        " "
    )

    # Remove unwanted control characters.
    # Preserve normal whitespace (already replaced above).
    cleaned = regexp_replace(
        cleaned,
        r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]",
        ""
    )

    # Replace multiple consecutive spaces with one space.
    cleaned = regexp_replace(
        cleaned,
        r" {2,}",
        " "
    )

    # Remove leading and trailing whitespace.
    cleaned = trim(cleaned)

    return cleaned


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    spark = (
        SparkSession.builder
        .appName("EgyptianArabicTTS-TextNormalization")
        .getOrCreate()
    )

    print("\n========== TEXT NORMALIZATION ==========\n")

    # 1. Read Bronze data
    bronze_df = (
        spark.read
        .schema(bronze_schema)
        .json(HDFS_PATH)
    )

    # 2. Preserve original text and create cleaned text
    cleaned_df = (
        bronze_df
        .withColumnRenamed("text", "original_text")
        .withColumn(
            "cleaned_text",
            normalize_text(col("original_text"))
        )
    )

    # 3. Display before-and-after examples
    print("\n--- Before and After Cleaning ---")

    cleaned_df.select(
        "record_id",
        "title",
        "original_text",
        "cleaned_text"
    ).show(10, truncate=100)

    # 4. Calculate text length before and after
    comparison_df = cleaned_df.select(
        length(col("original_text")).alias("original_length"),
        length(col("cleaned_text")).alias("cleaned_length")
    )

    print("\n--- Text Length Comparison ---")

    comparison_df.select(
        count("*").alias("total_records"),
        avg("original_length").alias("avg_original_length"),
        avg("cleaned_length").alias("avg_cleaned_length")
    ).show(truncate=False)

    # 5. Count records where text changed
    changed_records = cleaned_df.filter(
        col("original_text") != col("cleaned_text")
    ).count()

    unchanged_records = cleaned_df.filter(
        col("original_text") == col("cleaned_text")
    ).count()

    print(f"Records changed: {changed_records}")
    print(f"Records unchanged: {unchanged_records}")

    # 6. Validate cleaned text
    validation_report = cleaned_df.select(

        count(
            when(col("cleaned_text").isNull(), 1)
        ).alias("null_cleaned_text"),

        count(
            when(
                col("cleaned_text").isNotNull()
                & (trim(col("cleaned_text")) == ""),
                1
            )
        ).alias("empty_cleaned_text"),

        count(
            when(
                col("cleaned_text").rlike(r"  +"),
                1
            )
        ).alias("remaining_multiple_spaces"),

        count(
            when(
                col("cleaned_text").rlike(r"[\t\n\r]"),
                1
            )
        ).alias("remaining_tabs_or_linebreaks")

    )

    print("\n--- Cleaning Validation ---")
    validation_report.show(truncate=False)

    print("\n========== NORMALIZATION COMPLETED ==========\n")

    spark.stop()


if __name__ == "__main__":
    main()
