
from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("EgyptianArabicTTS-ConnectionTest")
        .getOrCreate()
    )

    print("Spark version:", spark.version)

    hdfs_path = (
        "hdfs://namenode:9000/data/bronze/wikipedia/"
        "ingestion_date=*/scraped_text_*.jsonl"
    )

    print("Reading Bronze data from HDFS...")

    df = spark.read.json(hdfs_path)

    print("Schema:")
    df.printSchema()

    print("First 5 records:")
    df.select("page_id", "title", "language").show(5, truncate=False)

    print("Total records:", df.count())

    spark.stop()


if __name__ == "__main__":
    main()
