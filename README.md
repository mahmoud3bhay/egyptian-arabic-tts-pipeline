# Egyptian Arabic TTS Data Pipeline

An end-to-end data engineering project designed to collect, process, and organize Egyptian Arabic text and speech data for Text-to-Speech (TTS) applications.

The project follows a data lake architecture using containerized services, distributed storage, and distributed data processing technologies.

## Project Objectives

- Collect Egyptian Arabic text from public sources.
- Build a scalable data ingestion pipeline.
- Store raw data in HDFS using a Bronze layer.
- Explore, clean, and validate text data using Apache Spark.
- Transform articles into structured text segments suitable for TTS generation.
- Generate speech audio using TTS models.
- Evaluate generated audio quality using ASR and audio metrics.
- Organize audio outputs and metadata in a Gold layer.

## Architecture

### Current implementation

**M1 — Data Ingestion**

Egyptian Arabic Wikipedia → Python Scraper → JSONL → HDFS Bronze

**M2 — Distributed Processing**

HDFS Bronze → PySpark → Data Cleaning and Validation → Text Segmentation → HDFS Silver (Parquet)

### Planned architecture

Data Sources → Python Ingestion → HDFS Bronze → PySpark Silver → TTS Generation → Audio Quality Assessment → HDFS Gold

## Technology Stack

| Technology              | Purpose                                                    |
| ----------------------- | ---------------------------------------------------------- |
| Python                  | Data ingestion and pipeline logic                          |
| Docker & Docker Compose | Containerization and service orchestration                 |
| Apache Hadoop HDFS      | Distributed data storage                                   |
| Apache Spark            | Distributed data processing                                |
| PySpark                 | Data exploration, cleaning, validation, and transformation |
| JSONL                   | Raw ingestion data format                                  |
| Apache Parquet          | Processed data storage format                              |
| MediaWiki API           | Source data extraction                                     |

TTS and ASR technologies will be introduced in subsequent milestones.

## Data Lake Structure

The project follows a medallion architecture:

| Layer  | Description                                    | Storage Format         |
| ------ | ---------------------------------------------- | ---------------------- |
| Bronze | Raw articles collected from the source         | JSONL                  |
| Silver | Cleaned, validated, and segmented text         | Parquet                |
| Gold   | Generated audio and quality assessment results | WAV + Parquet metadata |

### HDFS Directory Structure

```text
/data
├── bronze/
│   └── wikipedia/
│       └── ingestion_date=YYYY-MM-DD/
│           └── scraped_text_*.jsonl
│
├── silver/
│   └── segments/
│       └── *.parquet
│
└── gold/
    ├── accepted/
    ├── review/
    └── rejected/
```

The Gold layer is planned for a later milestone.

## M2 Processing Workflow

The distributed processing workflow includes:

1. Reading raw JSONL records from HDFS Bronze using PySpark.
2. Defining an explicit schema for the source data.
3. Exploring record counts, distributions, missing values, and text lengths.
4. Cleaning text by normalizing whitespace and removing unwanted control characters.
5. Validating records and identifying potentially unsuitable content.
6. Splitting articles into sentences.
7. Combining sentences into TTS-ready text segments.
8. Generating deterministic segment IDs.
9. Validating segment completeness, uniqueness, ordering, and source traceability.
10. Writing processed segments to HDFS Silver in Parquet format.

### Text Segmentation

The segmentation process uses configurable parameters:

- Minimum segment length: 50 characters.
- Target segment length: 300 characters.
- Maximum segment length: 500 characters.

Segments that exceed the maximum because of an individual long sentence are retained and flagged for review rather than silently truncated.

## Project Structure

```text
.
├── docker/
│   └── python/
│       └── Dockerfile
├── scraper/
│   └── main.py
├── storage/
│   ├── __init__.py
│   └── hdfs_client.py
├── spark/
│   └── jobs/
│       ├── connection_test.py
│       ├── explore_bronze.py
│       ├── clean_bronze.py
│       ├── clean_text.py
│       ├── validate_content.py
│       ├── split_sentences.py
│       └── build_segments.py
├── requirements.txt
├── docker-compose.yml
└── README.md
```

## Running the Project

### Prerequisites

- Docker Desktop
- Docker Compose
- Git

### Start the services

```bash
docker compose up -d --build
```

### Run the scraper

```bash
docker compose exec python python scraper/main.py
```

### Upload raw data to HDFS

```bash
docker compose exec python python storage/hdfs_client.py
```

### Verify Bronze data

```bash
docker compose exec namenode hdfs dfs -ls -R /data/bronze/wikipedia
```

### Run the Spark processing job

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/spark/jobs/build_segments.py
```

### Verify Silver data

```bash
docker compose exec namenode hdfs dfs -ls -R /data/silver/segments
```

### HDFS Web Interface

Open [http://localhost:9870](http://localhost:9870).

### Spark Web Interface

Open [http://localhost:8080](http://localhost:8080).

## Data Source and Licensing

The initial ingestion source is [Egyptian Arabic Wikipedia](https://arz.wikipedia.org/).

Wikipedia text is generally available under the Creative Commons Attribution-ShareAlike 4.0 International License (CC BY-SA 4.0), subject to the applicable licensing terms. Individual media files may have different licenses.

The pipeline retains source URLs and revision metadata to support data provenance and attribution.

## Project Milestones

- [x] M1 — Data ingestion and Bronze storage.
- [x] M2 — Distributed processing with PySpark and Silver storage.
- [ ] M3 — TTS generation.
- [ ] M4 — Audio quality assessment.
- [ ] M5 — Gold-layer storage and organization.

## Current Status

### M1 — Completed

The ingestion pipeline collects random articles from Egyptian Arabic Wikipedia using the MediaWiki API, extracts article metadata and text, stores the records locally in JSONL format, and uploads ingestion batches to HDFS Bronze.

### M2 — Processing implementation completed; final storage verification pending

The Spark processing environment is integrated with Docker and HDFS.

The pipeline supports data exploration, text cleaning, content validation, sentence splitting, and TTS-oriented segmentation. Generated segments include deterministic identifiers and source metadata.

The final Silver Parquet write and read-back validation are the remaining checks before M2 can be marked complete.

### Upcoming Work

The next milestone introduces TTS generation to convert validated Egyptian Arabic text segments into audio files while preserving the relationship between source records, text segments, and generated audio.
