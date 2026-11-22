
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    regexp_replace,
    length,
    size,
    split,
    when,
    count,
    avg,
    sum as spark_sum
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

MIN_TEXT_LENGTH = 100
LONG_TEXT_THRESHOLD = 3000
MIN_SENTENCE_COUNT = 2


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
# Text Normalization
# --------------------------------------------------

def normalize_text(text_column):

    cleaned = regexp_replace(
        text_column,
        r"[\t\n\r]+",
        " "
    )

    cleaned = regexp_replace(
        cleaned,
        r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]",
        ""
    )

    cleaned = regexp_replace(
        cleaned,
        r" {2,}",
        " "
    )

    return trim(cleaned)


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    spark = (
        SparkSession.builder
        .appName("EgyptianArabicTTS-ContentValidation")
        .getOrCreate()
    )

    print("\n========== CONTENT VALIDATION ==========\n")

    # 1. Read Bronze
    bronze_df = (
        spark.read
        .schema(bronze_schema)
        .json(HDFS_PATH)
    )

    # 2. Normalize text
    df = (
        bronze_df
        .withColumn(
            "cleaned_text",
            normalize_text(col("text"))
        )
    )

    # 3. Calculate content characteristics
    df = (
        df
        .withColumn(
            "text_length",
            length(col("cleaned_text"))
        )
        .withColumn(
            "sentence_count",
            size(
                split(
                    col("cleaned_text"),
                    r"[.!؟?]+"
                )
            )
        )
        .withColumn(
            "url_count",
            size(
                split(
                    col("cleaned_text"),
                    r"https?://"
                )
            ) - 1
        )
        .withColumn(
            "number_count",
            size(
                split(
                    col("cleaned_text"),
                    r"[0-9٠-٩]+"
                )
            ) - 1
        )
    )

    # 4. Hard validation rules
    df = (
        df
        .withColumn(
            "is_valid",
            (
                col("record_id").isNotNull()
                & (trim(col("record_id")) != "")
                & col("title").isNotNull()
                & (trim(col("title")) != "")
                & col("cleaned_text").isNotNull()
                & (col("text_length") >= MIN_TEXT_LENGTH)
                & col("page_id").isNotNull()
                & (col("page_id") > 0)
                & col("revision_id").isNotNull()
                & (col("revision_id") > 0)
            )
        )
    )

    # 5. Content quality flags
    df = (
        df
        .withColumn(
            "is_long_article",
            col("text_length") > LONG_TEXT_THRESHOLD
        )
        .withColumn(
            "low_sentence_count",
            col("sentence_count") < MIN_SENTENCE_COUNT
        )
        .withColumn(
            "has_many_urls",
            col("url_count") > 3
        )
        .withColumn(
            "has_many_numbers",
            col("number_count") > 20
        )
    )
    
    # Classify content based on validation results
    df = df.withColumn(
        "content_status",
        when(
            ~col("is_valid"),
            "rejected"
        ).when(
            col("has_many_numbers") | col("is_long_article"),
            "review"
        ).otherwise(
            "accepted"
        )
    )

    # Display content classification
    print("--- Content Classification ---")

    df.groupBy("content_status").count().show()

    # 6. Overall validation report
    total_records = df.count()

    report = df.select(
        count("*").alias("total_records"),

        count(
            when(col("is_valid"), 1)
        ).alias("valid_records"),

        count(
            when(~col("is_valid"), 1)
        ).alias("invalid_records"),

        count(
            when(col("is_long_article"), 1)
        ).alias("long_articles"),

        count(
            when(col("low_sentence_count"), 1)
        ).alias("low_sentence_count_articles"),

        count(
            when(col("has_many_urls"), 1)
        ).alias("articles_with_many_urls"),

        count(
            when(col("has_many_numbers"), 1)
        ).alias("articles_with_many_numbers")
    )

    print("--- Content Validation Report ---")
    report.show(truncate=False)

    # 7. Text length statistics
    print("--- Text Length Statistics ---")

    df.select(
        avg("text_length").alias("average_length"),
        spark_sum(
            when(col("text_length") < MIN_TEXT_LENGTH, 1).otherwise(0)
        ).alias("below_minimum_length")
    ).show(truncate=False)

    # 8. Inspect invalid records
    print("--- Invalid Records ---")

    df.filter(~col("is_valid")).select(
        "record_id",
        "title",
        "text_length",
        "page_id",
        "revision_id"
    ).show(20, truncate=False)

    # 9. Inspect suspicious content
    print("--- Suspicious Content (Review Required) ---")

    df.filter(
        col("is_valid")
        & (
            col("is_long_article")
            | col("low_sentence_count")
            | col("has_many_urls")
            | col("has_many_numbers")
        )
    ).select(
        "record_id",
        "title",
        "text_length",
        "sentence_count",
        "url_count",
        "number_count"
    ).show(20, truncate=False)
    df.select(
        "record_id",
        "title",
        "content_status",
        "text_length",
        "number_count"
    ).filter(
        col("content_status") == "review"
    ).show(20, truncate=False)

    # 10. Separate valid and invalid DataFrames
    valid_df = df.filter(col("is_valid"))
    invalid_df = df.filter(~col("is_valid"))

    print(f"Total records: {total_records}")
    print(f"Valid records: {valid_df.count()}")
    print(f"Invalid records: {invalid_df.count()}")
    
    # Inspect articles with many numbers
    print("--- Articles with Many Numbers ---")

    df.filter(
        col("has_many_numbers")
    ).select(
        "record_id",
        "title",
        "number_count",
        "cleaned_text"
    ).show(10, truncate=False)

    print("\n========== CONTENT VALIDATION COMPLETED ==========\n")
    
    # --------------------------------------------------
    # Final Validation
    # --------------------------------------------------

    print("\n========== FINAL VALIDATION ==========\n")

    # 1. Total record count
    print("Total records:", df.count())

    # 2. Content status distribution
    df.groupBy("content_status").count().show()

    # 3. Check for invalid content statuses
    invalid_status_count = df.filter(
        ~col("content_status").isin(
            "accepted", "review", "rejected"
        )
    ).count()

    print("Invalid content statuses:", invalid_status_count)

    # 4. Validate accepted records
    invalid_accepted_records = df.filter(
        (col("content_status") == "accepted")
        & (
            col("record_id").isNull()
            | col("title").isNull()
            | col("cleaned_text").isNull()
            | (col("text_length") < MIN_TEXT_LENGTH)
        )
    ).count()

    print("Invalid accepted records:", invalid_accepted_records)

    # 5. Check duplicate IDs
    duplicate_ids = (
        df.groupBy("record_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    print("Duplicate record ID groups:", duplicate_ids)

    # 6. Verify classification totals
    classification_total = (
        df.groupBy("content_status")
        .count()
        .agg(spark_sum("count").alias("total"))
        .first()["total"]
    )

    print("Classification total:", classification_total)

    print("\n========== FINAL VALIDATION COMPLETED ==========\n")

    spark.stop()


if __name__ == "__main__":
    main()
