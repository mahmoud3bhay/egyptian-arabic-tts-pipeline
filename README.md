# Egyptian Arabic TTS Data Pipeline

An end-to-end data engineering project designed to collect, process, and organize Egyptian Arabic text and speech data for Text-to-Speech (TTS) applications.

The project follows a data lake architecture and uses distributed storage and processing technologies.

## Project Objectives

- Collect Egyptian Arabic text from public sources.
- Build a scalable data ingestion pipeline.
- Store raw data in HDFS using a Bronze layer.
- Process and clean text using Apache Spark.
- Generate speech audio using TTS models.
- Evaluate generated audio quality using ASR and audio metrics.

## Architecture

**Current implementation (M1):**

Egyptian Arabic Wikipedia → Python Scraper → JSONL → HDFS (Bronze)

**Planned architecture:**

Sources → Python Ingestion → HDFS Bronze → PySpark Silver → TTS Generation → Audio Quality Assessment → HDFS Gold

## Technology Stack

| Technology              | Purpose                                    |
| ----------------------- | ------------------------------------------ |
| Python                  | Data ingestion and pipeline logic          |
| Docker & Docker Compose | Containerization and service orchestration |
| Apache Hadoop HDFS      | Distributed data storage                   |
| JSONL                   | Raw data storage format                    |
| MediaWiki API           | Source data extraction                     |

Apache Spark and TTS/ASR technologies will be introduced in subsequent milestones.

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
├── requirements.txt
├── docker-compose.yml
└── README.md
```

## Milestones

- [x] M1 — Data ingestion and Bronze storage
- [ ] M2 — Distributed processing with PySpark
- [ ] M3 — TTS generation
- [ ] M4 — Audio quality assessment
- [ ] M5 — Gold-layer storage and organization

## Running the Project

### Prerequisites

- Docker Desktop
- Docker Compose

### Start the services

```bash
docker compose up -d --build
```

### Run the scraper

```bash
docker compose exec python python scraper/main.py
```

### Upload data to HDFS

```bash
docker compose exec python python storage/hdfs_client.py
```

### Verify HDFS data

```bash
docker compose exec namenode hdfs dfs -ls -R /data/bronze/wikipedia
```

### HDFS Web Interface

Open [http://localhost:9870](http://localhost:9870).

## Data Source and Licensing

The initial ingestion source is [Egyptian Arabic Wikipedia](https://arz.wikipedia.org/).

Wikipedia text is generally available under the Creative Commons Attribution-ShareAlike 4.0 International License (CC BY-SA 4.0), subject to the applicable licensing terms. Individual media files may have different licenses.

The pipeline retains source URLs and revision metadata to support data provenance and attribution.

## Current Status

M1 is implemented. The pipeline can collect random Egyptian Arabic Wikipedia articles, save them locally as JSONL, and upload ingestion batches to HDFS.

Further milestones will extend the pipeline with distributed processing, speech generation, and quality assessment.
