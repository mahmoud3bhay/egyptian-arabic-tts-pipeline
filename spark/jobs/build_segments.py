
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
)
from pyspark.sql.functions import (
    col,
    when,
    trim,
    length,
    regexp_replace,
    split,
    posexplode,
    collect_list,
    struct,
    sort_array,
)

# ============================================================
# 1. Configuration
# ============================================================

MIN_SEGMENT_LENGTH = 50
TARGET_SEGMENT_LENGTH = 300
MAX_SEGMENT_LENGTH = 500

HDFS_PATH = (
    "hdfs://namenode:9000/data/bronze/wikipedia/"
    "ingestion_date=*/scraped_text_*.jsonl"
)

# ============================================================
# 2. Spark session
# ============================================================

spark = (
    SparkSession.builder
    .appName("EgyptianArabicTTS-BuildSegments")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 3. Bronze schema
# ============================================================

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
    StructField("collection_timestamp", StringType(), True),
])


# ============================================================
# 4. Read Bronze data
# ============================================================

bronze_df = (
    spark.read
    .schema(bronze_schema)
    .json(HDFS_PATH)
)

print("Bronze records:", bronze_df.count())


# ============================================================
# 5. Normalize text
# ============================================================

cleaned_df = (
    bronze_df
    .withColumn(
        "cleaned_text",
        regexp_replace(col("text"), r"[\t\r\n]", " ")
    )
    .withColumn(
        "cleaned_text",
        regexp_replace(col("cleaned_text"), r"[\p{Cc}]", "")
    )
    .withColumn(
        "cleaned_text",
        regexp_replace(col("cleaned_text"), r"\s+", " ")
    )
    .withColumn("cleaned_text", trim(col("cleaned_text")))
    .withColumn("text_length", length(col("cleaned_text")))
)


# ============================================================
# 6. Validate articles and classify content
# ============================================================

# Count sequences of Western, Arabic-Indic, and Persian digits.
cleaned_df = cleaned_df.withColumn(
    "number_count",
    F.size(
        split(
            col("cleaned_text"),
            r"[0-9٠-٩۰-۹]+"
        )
    ) - 1
)

cleaned_df = cleaned_df.withColumn(
    "is_valid",
    (
        col("record_id").isNotNull()
        & (length(trim(col("record_id"))) > 0)
        & col("title").isNotNull()
        & (length(trim(col("title"))) > 0)
        & col("cleaned_text").isNotNull()
        & (col("text_length") >= 100)
        & col("page_id").isNotNull()
        & (col("page_id") > 0)
        & col("revision_id").isNotNull()
        & (col("revision_id") > 0)
    )
)

cleaned_df = cleaned_df.withColumn(
    "content_status",
    when(~col("is_valid"), "rejected")
    .when(
        (col("text_length") > 3000)
        | (col("number_count") > 20),
        "review"
    )
    .otherwise("accepted")
)

valid_articles = cleaned_df.filter(col("is_valid"))


# ============================================================
# 7. Split articles into sentences
# ============================================================

sentences_df = (
    valid_articles
    .select(
        "record_id",
        "title",
        "content_status",
        posexplode(
            split(
                col("cleaned_text"),
                r"(?<=[.!؟?])\s+"
            )
        ).alias("sentence_position", "sentence_text")
    )
    .withColumn("sentence_text", trim(col("sentence_text")))
    .filter(length(col("sentence_text")) > 0)
    .withColumn(
        "sentence_index",
        (col("sentence_position") + 1).cast("int")
    )
    .withColumn(
        "sentence_length",
        length(col("sentence_text"))
    )
    .drop("sentence_position")
)

print("Extracted sentences:", sentences_df.count())


# ============================================================
# 8. Group sentences by article and preserve their order
# ============================================================

grouped_articles = (
    sentences_df
    .groupBy(
        "record_id",
        "title",
        "content_status"
    )
    .agg(
        sort_array(
            collect_list(
                struct(
                    col("sentence_index"),
                    col("sentence_text"),
                    col("sentence_length")
                )
            )
        ).alias("sentences")
    )
)


# ============================================================
# 9. Define output schema
# ============================================================

segment_schema = StructType([
    StructField("record_id", StringType(), False),
    StructField("title", StringType(), False),
    StructField("article_content_status", StringType(), False),
    StructField("segment_index", IntegerType(), False),
    StructField("segment_text", StringType(), False),
    StructField("segment_length", IntegerType(), False),
    StructField("sentence_count", IntegerType(), False),
    StructField("start_sentence_index", IntegerType(), False),
    StructField("end_sentence_index", IntegerType(), False),
    StructField("segment_status", StringType(), False),
])


# ============================================================
# 10. Greedy segmentation function
# ============================================================

def build_article_segments(article):
    """
    Build segments for one article.

    Sentences are added in order until adding the next sentence
    would exceed MAX_SEGMENT_LENGTH.

    An individual sentence longer than the maximum is retained
    as a separate segment and flagged for review.
    """

    record_id = article.record_id
    title = article.title
    article_status = article.content_status

    sentences = article.sentences

    current_sentences = []
    current_length = 0
    segments = []

    def save_segment(sentence_list, segment_index):
        segment_text = " ".join(
            sentence.sentence_text
            for sentence in sentence_list
        )

        segment_length = len(segment_text)

        if segment_length > MAX_SEGMENT_LENGTH:
            status = "review_oversized_sentence"
        elif segment_length < MIN_SEGMENT_LENGTH:
            status = "review_short_segment"
        else:
            status = "ready"

        return (
            record_id,
            title,
            article_status,
            segment_index,
            segment_text,
            segment_length,
            len(sentence_list),
            sentence_list[0].sentence_index,
            sentence_list[-1].sentence_index,
            status,
        )

    for sentence in sentences:

        sentence_text = sentence.sentence_text
        sentence_length = sentence.sentence_length

        # Case 1: An individual sentence exceeds the maximum.
        # Save the current segment first, then keep this sentence
        # separately without truncating it.

        if sentence_length > MAX_SEGMENT_LENGTH:

            if current_sentences:
                segments.append(
                    save_segment(
                        current_sentences,
                        len(segments) + 1
                    )
                )

                current_sentences = []
                current_length = 0

            segments.append(
                save_segment(
                    [sentence],
                    len(segments) + 1
                )
            )

            continue

        # Case 2: The current segment is empty.
        if not current_sentences:
            current_sentences = [sentence]
            current_length = sentence_length
            continue

        # Case 3: Check the cumulative length before adding.
        # Include one space between the existing segment and
        # the new sentence.

        proposed_length = current_length + 1 + sentence_length

        if proposed_length <= MAX_SEGMENT_LENGTH:

            current_sentences.append(sentence)
            current_length = proposed_length

        else:
            # Save the previous segment and start another one.

            segments.append(
                save_segment(
                    current_sentences,
                    len(segments) + 1
                )
            )

            current_sentences = [sentence]
            current_length = sentence_length

    # Save the final segment of the article.
    if current_sentences:
        segments.append(
            save_segment(
                current_sentences,
                len(segments) + 1
            )
        )

    return segments


# ============================================================
# 11. Execute segmentation on Spark workers
# ============================================================

segments_rdd = grouped_articles.rdd.flatMap(
    build_article_segments
)

segments_df = spark.createDataFrame(
    segments_rdd,
    schema=segment_schema
)

# ============================================================
# 11.1 Generate unique segment IDs
# ============================================================

segments_df = segments_df.withColumn(
    "segment_id",
    F.concat(
        col("record_id"),
        F.lit("_"),
        F.lpad(
            col("segment_index").cast("string"),
            4,
            "0"
        )
    )
)

print("\nGenerated segment IDs:")

segments_df.select(
    "segment_id",
    "record_id",
    "segment_index",
    "segment_length"
).show(10, truncate=False)


# ============================================================
# 11.2 Validate segment IDs
# ============================================================

# Check for null or empty IDs
invalid_ids = segments_df.filter(
    col("segment_id").isNull()
    | (F.length(col("segment_id")) == 0)
)

print("Invalid segment IDs:", invalid_ids.count())


# Check uniqueness
duplicate_ids = (
    segments_df
    .groupBy("segment_id")
    .count()
    .filter(col("count") > 1)
)

print("Duplicate segment IDs:", duplicate_ids.count())


# Check reproducible ID structure
invalid_id_format = segments_df.filter(
    col("segment_id") != F.concat(
        col("record_id"),
        F.lit("_"),
        F.lpad(
            col("segment_index").cast("string"),
            4,
            "0"
        )
    )
)

print("Invalid ID formats:", invalid_id_format.count())


# Check total number of IDs
print(
    "Total segment IDs:",
    segments_df.select("segment_id").distinct().count()
)



# ============================================================
# 12. Inspect results
# ============================================================

print("\nSample segments:")

segments_df.select(
    "record_id",
    "segment_index",
    "segment_length",
    "sentence_count",
    "segment_status",
    "segment_text"
).show(10, truncate=100)


# ============================================================
# 13. Segment statistics
# ============================================================

print("\nSegment length statistics:")

segments_df.select(
    F.count("*").alias("total_segments"),
    F.min("segment_length").alias("min_length"),
    F.round(F.avg("segment_length"), 2).alias("avg_length"),
    F.max("segment_length").alias("max_length"),
).show()


print("\nSegment status distribution:")

segments_df.groupBy(
    "segment_status"
).count().orderBy(
    "segment_status"
).show()


print("\nSegment length distribution:")

segments_df.withColumn(
    "length_category",
    when(col("segment_length") < MIN_SEGMENT_LENGTH, "below_50")
    .when(col("segment_length") <= TARGET_SEGMENT_LENGTH, "50_to_300")
    .when(col("segment_length") <= MAX_SEGMENT_LENGTH, "301_to_500")
    .otherwise("above_500")
).groupBy(
    "length_category"
).count().orderBy(
    "length_category"
).show()

# Inspect oversized segments

print("\nOversized segments:")

segments_df.filter(
    F.col("segment_length") > MAX_SEGMENT_LENGTH
).select(
    "record_id",
    "title",
    "segment_index",
    "segment_length",
    "sentence_count",
    "segment_text"
).orderBy(
    F.desc("segment_length")
).show(30, truncate=150)


# Inspect short segments

print("\nShort segments:")

segments_df.filter(
    F.col("segment_length") < MIN_SEGMENT_LENGTH
).select(
    "record_id",
    "title",
    "segment_index",
    "segment_length",
    "sentence_count",
    "segment_text"
).show(20, truncate=150)



# ============================================================
# 14. Validate segmentation
# ============================================================

# No normal segment should exceed the maximum.
# Oversized individual sentences are explicitly flagged.

invalid_segments = segments_df.filter(
    (
        col("segment_length") > MAX_SEGMENT_LENGTH
    )
    & (
        col("segment_status") != "review_oversized_sentence"
    )
)

print(
    "Unexpected oversized segments:",
    invalid_segments.count()
)

# Check segment ordering and uniqueness within each article.

duplicate_segments = (
    segments_df
    .groupBy("record_id", "segment_index")
    .count()
    .filter(col("count") > 1)
)

print(
    "Duplicate segment indexes:",
    duplicate_segments.count()
)

print(
    "Total articles represented:",
    segments_df.select("record_id").distinct().count()
)

print(
    "Total generated segments:",
    segments_df.count()
)


# ============================================================
# 15. Finish
# ============================================================

# ============================================================
# FINAL M2.4.4 - SEGMENT VALIDATION
# ============================================================

print("\n========== FINAL SEGMENT VALIDATION ==========")

# ------------------------------------------------------------
# 1. Completeness validation
# ------------------------------------------------------------

invalid_records = segments_df.filter(
    col("segment_id").isNull()
    | (F.length(F.trim(col("segment_id"))) == 0)
    | col("record_id").isNull()
    | col("title").isNull()
    | col("segment_text").isNull()
    | (F.length(F.trim(col("segment_text"))) == 0)
    | col("segment_index").isNull()
    | col("segment_length").isNull()
)

print("Invalid records:", invalid_records.count())


# ------------------------------------------------------------
# 2. Uniqueness validation
# ------------------------------------------------------------

duplicate_ids = (
    segments_df
    .groupBy("segment_id")
    .count()
    .filter(col("count") > 1)
)

print("Duplicate segment IDs:", duplicate_ids.count())


# ------------------------------------------------------------
# 3. Length consistency validation
# ------------------------------------------------------------

invalid_lengths = segments_df.filter(
    col("segment_length") != F.length(col("segment_text"))
)

print("Invalid segment lengths:", invalid_lengths.count())


# ------------------------------------------------------------
# 4. Segment ordering validation
# ------------------------------------------------------------

ordering_stats = (
    segments_df
    .groupBy("record_id")
    .agg(
        F.count("*").alias("segment_count"),
        F.min("segment_index").alias("min_index"),
        F.max("segment_index").alias("max_index"),
        F.countDistinct("segment_index").alias("distinct_indexes")
    )
    .filter(
        (col("min_index") != 1)
        | (col("max_index") != col("segment_count"))
        | (col("distinct_indexes") != col("segment_count"))
    )
)

print("Articles with invalid segment ordering:", ordering_stats.count())


# ------------------------------------------------------------
# 5. Traceability validation
# ------------------------------------------------------------

bronze_ids = (
    bronze_df
    .select("record_id")
    .distinct()
)

unmatched_segments = (
    segments_df
    .select("record_id")
    .distinct()
    .join(
        bronze_ids,
        on="record_id",
        how="left_anti"
    )
)

print("Segments with missing Bronze references:", unmatched_segments.count())


# ------------------------------------------------------------
# 6. Segment status validation
# ------------------------------------------------------------

invalid_statuses = segments_df.filter(
    ~col("segment_status").isin(
        "ready",
        "review_short_segment",
        "review_oversized_sentence"
    )
)

print("Invalid segment statuses:", invalid_statuses.count())


# ------------------------------------------------------------
# 7. Final summary
# ------------------------------------------------------------

print("\n========== FINAL SUMMARY ==========")

print("Total articles:", bronze_ids.count())

print("Articles represented:",
      segments_df.select("record_id").distinct().count())

print("Total segments:", segments_df.count())

print("Distinct segment IDs:",
      segments_df.select("segment_id").distinct().count())

print("Ready segments:",
      segments_df.filter(col("segment_status") == "ready").count())

print("Short segments:",
      segments_df.filter(
          col("segment_status") == "review_short_segment"
      ).count())

print("Oversized sentences:",
      segments_df.filter(
          col("segment_status") == "review_oversized_sentence"
      ).count())

print("\n========== VALIDATION COMPLETED ==========")
#-----------------------------------------------------------------------------

# ============================================================
# M2.5.1 - Prepare Silver DataFrame
# ============================================================

silver_metadata = bronze_df.select(
    "record_id",
    "source_name",
    "source_url",
    "page_id",
    "revision_id",
    "revision_timestamp",
    "language",
    "dialect",
    "license",
    "collection_timestamp"
)

segments_silver_df = segments_df.join(
    silver_metadata,
    on="record_id",
    how="left"
)

print("\nSilver DataFrame schema:")
segments_silver_df.printSchema()

print("\nSilver DataFrame sample:")
segments_silver_df.show(5, truncate=100)


# ============================================================
# M2.5.2 - Write Silver Data to HDFS
# ============================================================

SILVER_PATH = (
    "hdfs://namenode:9000/data/silver/segments"
)

segments_silver_df.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)

print("\nSilver data successfully written to HDFS:")
print(SILVER_PATH)



# ============================================================
# M2.5.3 - Read Back and Validate Silver Data
# ============================================================

silver_df = spark.read.parquet(SILVER_PATH)

print("\nSilver records:", silver_df.count())

print("\nSilver schema:")
silver_df.printSchema()

print("\nSilver sample:")
silver_df.select(
    "segment_id",
    "record_id",
    "segment_index",
    "segment_length",
    "segment_status",
    "language",
    "dialect"
).show(10, truncate=False)


# Compare Bronze processing output with Silver
source_count = segments_silver_df.count()
silver_count = silver_df.count()

print("\nSource segment count:", source_count)
print("Silver segment count:", silver_count)

print(
    "Record count matches:",
    source_count == silver_count
)


# Check duplicate IDs after reading
duplicate_silver_ids = (
    silver_df
    .groupBy("segment_id")
    .count()
    .filter(F.col("count") > 1)
)

print(
    "Duplicate Silver IDs:",
    duplicate_silver_ids.count()
)


# Check missing segment text
missing_text = silver_df.filter(
    col("segment_text").isNull()
    | (F.length(F.trim(col("segment_text"))) == 0)
)

print("Missing segment text:", missing_text.count())


spark.stop()
