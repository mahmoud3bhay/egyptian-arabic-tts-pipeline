
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    regexp_replace,
    length,
    split,
    posexplode,
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

MIN_TEXT_LENGTH = 100
MAX_SENTENCE_LENGTH = 500


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
        .appName("EgyptianArabicTTS-SentenceSplitting")
        .getOrCreate()
    )

    print("\n========== SENTENCE SPLITTING ==========\n")

    # 1. Read Bronze data
    bronze_df = (
        spark.read
        .schema(bronze_schema)
        .json(HDFS_PATH)
    )

    # 2. Normalize text
    df = bronze_df.withColumn(
        "cleaned_text",
        normalize_text(col("text"))
    )

    # 3. Apply hard validation rules
    df = df.withColumn(
        "is_valid",
        (
            col("record_id").isNotNull()
            & (trim(col("record_id")) != "")
            & col("title").isNotNull()
            & (trim(col("title")) != "")
            & col("cleaned_text").isNotNull()
            & (length(col("cleaned_text")) >= MIN_TEXT_LENGTH)
            & col("page_id").isNotNull()
            & (col("page_id") > 0)
            & col("revision_id").isNotNull()
            & (col("revision_id") > 0)
        )
    )

    # 4. Recreate content classification
    df = df.withColumn(
        "number_count",
        length(
            regexp_replace(
                col("cleaned_text"),
                r"[^0-9٠-٩]+",
                ""
            )
        )
    )

    df = df.withColumn(
        "content_status",
        when(
            ~col("is_valid"),
            "rejected"
        ).when(
            (length(col("cleaned_text")) > 3000)
            | (col("number_count") > 20),
            "review"
        ).otherwise(
            "accepted"
        )
    )

    # 5. Keep valid articles for sentence extraction
    valid_df = df.filter(col("is_valid"))

    # 6. Split articles into sentences
    sentences_df = (
        valid_df
        .select(
            "record_id",
            "title",
            "content_status",
            posexplode(
                split(
                    col("cleaned_text"),
                    r"(?<=[.!؟?])\s+"
                )
            ).alias("sentence_index", "sentence_text")
        )
        .withColumn(
            "sentence_text",
            trim(col("sentence_text"))
        )
        .filter(col("sentence_text") != "")
        .withColumn(
            "sentence_length",
            length(col("sentence_text"))
        )
    )

    # 7. Display examples
    print("--- Extracted Sentences ---")

    sentences_df.select(
        "record_id",
        "title",
        "sentence_index",
        "sentence_text",
        "sentence_length",
        "content_status"
    ).show(20, truncate=False)

    # 8. Sentence statistics
    print("--- Sentence Statistics ---")

    sentences_df.select(
        length("sentence_text").alias("sentence_length")
    ).summary("count", "mean", "min", "max").show()

    # 9. Check long sentences
    print("--- Sentences Exceeding Maximum Length ---")

    sentences_df.filter(
        col("sentence_length") > MAX_SENTENCE_LENGTH
    ).select(
        "record_id",
        "title",
        "sentence_length",
        "sentence_text"
    ).show(10, truncate=False)

    # 10. Count extracted sentences
    total_sentences = sentences_df.count()

    print(f"Total valid articles: {valid_df.count()}")
    print(f"Total extracted sentences: {total_sentences}")

    print("\n========== SENTENCE SPLITTING COMPLETED ==========\n")

    spark.stop()


if __name__ == "__main__":
    main()
