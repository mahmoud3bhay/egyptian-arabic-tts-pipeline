
from hdfs import InsecureClient
from pathlib import Path
from datetime import datetime, timezone


HDFS_URL = "http://namenode:9870"
HDFS_USER = "root"

HDFS_BASE_PATH = "/data/bronze/wikipedia"


def get_hdfs_client():
    """Create an HDFS client using WebHDFS."""
    return InsecureClient(
        HDFS_URL,
        user=HDFS_USER
    )


def upload_file(local_file_path):
    """Upload a local file to HDFS without overwriting previous batches."""

    local_file_path = Path(local_file_path)

    if not local_file_path.exists():
        raise FileNotFoundError(
            f"Local file not found: {local_file_path}"
        )

    client = get_hdfs_client()

    # Use a UTC timestamp to identify this ingestion batch.
    now = datetime.now(timezone.utc)
    ingestion_date = now.strftime("%Y-%m-%d")
    batch_timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    hdfs_directory = (
        f"{HDFS_BASE_PATH}/ingestion_date={ingestion_date}"
    )

    hdfs_file_path = (
        f"{hdfs_directory}/"
        f"scraped_text_{batch_timestamp}.jsonl"
    )

    # Create the destination directory if it doesn't exist.
    client.makedirs(hdfs_directory)

    # Upload the file.
    client.upload(
        hdfs_file_path,
        str(local_file_path),
        overwrite=False
    )

    print("Upload completed successfully!")
    print(f"Local file: {local_file_path}")
    print(f"HDFS file: {hdfs_file_path}")

    return hdfs_file_path


if __name__ == "__main__":
    local_path = "data/raw/scraped_text.jsonl"
    upload_file(local_path)
