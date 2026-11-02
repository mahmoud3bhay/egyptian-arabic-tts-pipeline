
import requests
import json
import time
import re
import hashlib
from datetime import datetime, timezone
from pathlib import Path


API_URL = "https://arz.wikipedia.org/w/api.php"

# Replace this with your real contact email or project URL.
USER_AGENT = (
    "EgyptianArabicTTSPipeline/1.0 "
    "(contact: your-email@example.com)"
)

MAX_ARTICLES = 100
BATCH_SIZE = 20
REQUEST_DELAY = 1

OUTPUT_FILE = Path("data/raw/scraped_text.jsonl")

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


def fetch_random_articles(batch_size):
    """Fetch random articles and their metadata in one API request."""

    params = {
        "action": "query",
        "generator": "random",
        "grnnamespace": 0,
        "grnfilterredir": "nonredirects",
        "grnlimit": batch_size,
        "prop": "extracts|info|revisions",
        "explaintext": 1,
        "exsectionformat": "plain",
        "inprop": "url",
        "rvprop": "ids|timestamp",
        "format": "json"
    }

    response = session.get(API_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    if "error" in data:
        raise RuntimeError(data["error"])

    pages = data.get("query", {}).get("pages", {})

    return list(pages.values())


def clean_text(text):
    if not text:
        return ""

    text = re.sub(r"\s+", " ", text)
    return text.strip()


def validate_article(article_text):
    return len(article_text) >= 100


def create_record(article):
    text = clean_text(article.get("extract", ""))

    revision = article.get("revisions", [{}])[0]

    record_id = hashlib.sha256(
        str(article["pageid"]).encode("utf-8")
    ).hexdigest()

    return {
        "record_id": record_id,
        "title": article.get("title"),
        "text": text,
        "source_name": "Egyptian Arabic Wikipedia",
        "source_url": article.get("fullurl"),
        "source_type": "wikipedia",
        "page_id": article.get("pageid"),
        "revision_id": revision.get("revid"),
        "revision_timestamp": revision.get("timestamp"),
        "language": "arz",
        "dialect": "egyptian",
        "license": "CC BY-SA 4.0",
        "collection_timestamp": datetime.now(
            timezone.utc
        ).isoformat()
    }


def save_jsonl(records):
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_FILE.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(
                json.dumps(record, ensure_ascii=False) + "\n"
            )

    print(f"Saved {len(records)} records to {OUTPUT_FILE}")


def main():
    collected_records = []
    seen_page_ids = set()

    print("Starting random article collection...")

    while len(collected_records) < MAX_ARTICLES:

        remaining = MAX_ARTICLES - len(collected_records)
        batch_size = min(BATCH_SIZE, remaining)

        articles = fetch_random_articles(batch_size)

        for article in articles:

            page_id = article.get("pageid")

            # Skip missing or previously collected articles.
            if not page_id or page_id in seen_page_ids:
                continue

            seen_page_ids.add(page_id)

            text = clean_text(article.get("extract", ""))

            if not validate_article(text):
                print(f"Skipped short article: {article.get('title')}")
                continue

            record = create_record(article)
            collected_records.append(record)

            print(
                f"[{len(collected_records)}/{MAX_ARTICLES}] "
                f"Collected: {record['title']}"
            )

            if len(collected_records) >= MAX_ARTICLES:
                break

        time.sleep(REQUEST_DELAY)

    save_jsonl(collected_records)

    print("Scraping completed successfully.")


if __name__ == "__main__":
    main()
