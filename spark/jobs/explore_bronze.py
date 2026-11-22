
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct
from pyspark.sql.functions import col, count, when, length
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)


def main():

    spark = (
        SparkSession.builder
        .appName("EgyptianArabicTTS-BronzeExploration")
        .getOrCreate()
    )

    # 1. Define the Bronze schema
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

    # 2. Read Bronze data from HDFS
    hdfs_path = (
        "hdfs://namenode:9000/data/bronze/wikipedia/"
        "ingestion_date=*/scraped_text_*.jsonl"
    )

    df = (
        spark.read
        .schema(bronze_schema)
        .json(hdfs_path)
    )
    #--------------2

    # 3. Explore the DataFrame
    print("DataFrame Schema:")
    df.printSchema()

    print("Sample Records:")
    df.show(5, truncate=False)

    print("Total Records:", df.count())

    # 1. Display column names
    print("Columns:")
    print(df.columns)

    # 2. Display DataFrame dimensions
    print("Number of columns:", len(df.columns))
    print("Number of records:", df.count())

    # 3. Count distinct records
    print("Distinct record IDs:", df.select(
        countDistinct("record_id")
    ).first()[0])

    # 4. Explore language distribution
    print("Language distribution:")
    df.groupBy("language").count().show()

    # 5. Explore dialect distribution
    print("Dialect distribution:")
    df.groupBy("dialect").count().show()

    # 6. Explore source distribution
    print("Source distribution:")
    df.groupBy("source_name").count().show()
    
    #------------------3

    

    # Count null values in every column
    print("Null values per column:")

    df.select([
        count(
            when(col(c).isNull(), c)
        ).alias(c)
        for c in df.columns
    ]).show(truncate=False)


    # Count empty strings in important text columns
    print("Empty values in text columns:")

    text_columns = [
        "record_id",
        "title",
        "text",
        "source_url"
    ]

    df.select([
        count(
            when(
                col(c).isNull() | (length(col(c)) == 0),
                c
            )
        ).alias(c)
        for c in text_columns
    ]).show()


    # Explore article length
    print("Article text length statistics:")

    df.select(
        length(col("text")).alias("text_length")
    ).describe().show()
    
    #--------------------4
    
    

    # Transformation 1: Filter articles longer than 1000 characters
    long_articles = df.filter(length(col("text")) > 1000)

    # Transformation 2: Select relevant columns
    selected_articles = long_articles.select(
        "record_id",
        "title",
        "text"
    )

    print("Execution plan:")
    selected_articles.explain()

    # Action: Trigger execution
    print("Long articles:")
    selected_articles.show(10, truncate=False)

    print("Number of long articles:", selected_articles.count())


    
    spark.stop()
    


if __name__ == "__main__":
    main()
