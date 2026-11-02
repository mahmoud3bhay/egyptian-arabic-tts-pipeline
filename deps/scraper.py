
import json
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests


API_URL = "https://arz.wikipedia.org/w/api.php"

# Start small. We can increase this after testing.
MAX_ARTICLES = 100
BATCH_SIZE = 20
REQUEST_DELAY = 1

OUTPUT_FILE = Path("/app/data/raw/scraped_text.jsonl")

HEADERS = {
    # Replace the contact information with your own.
    "User-Agent": (
        "EgyptianArabicTTSPipeline/1.0 "
        "(your-email@example.com) "
        "Python/requests"
    )
}

session = requests.Session()
session.headers.update(HEADERS)


def get_article_titles(limit):
    """Retrieve article titles using MediaWiki pagination."""

    titles = []
    continuation = {}

    while len(titles) < limit:

        params = {
            "action": "query",
            "list": "random",
            "apnamespace": 0,
            "apfilterredir": "nonredirects",
            "aplimit": min(50, limit - len(titles)),
            "format": "json",
            **continuation
        }

        response = session.get(
            API_URL,
            params=params,
            timeout=30
        )
        response.raise_for_status()

        data = response.json()

        for page in data["query"]["allpages"]:
            titles.append(page["title"])

        continuation = data.get("continue", {})

        if not continuation:
            break

        time.sleep(REQUEST_DELAY)

    return titles


def fetch_articles(titles):
    """Fetch article text and metadata in batches."""

    params = {
        "action": "query",
        "prop": "extracts|info|revisions",
        "titles": "|".join(titles),
        "explaintext": 1,
        "exsectionformat": "plain",
        "inprop": "url",
        "rvprop": "ids|timestamp",
        "format": "json"
    }

    response = session.get(
        API_URL,
        params=params,
        timeout=60
    )
    response.raise_for_status()

    return response.json()["query"]["pages"].values()


def clean_text(text):
    """Perform basic whitespace normalization."""

    text = re.sub(r"\s+", " ", text)
    return text.strip()


def validate_article(text):
    """Reject empty or very short articles."""

    return len(text) >= 100


def create_record(page):
    """Convert a Wikipedia page into our standard schema."""

    text = clean_text(page.get("extract", ""))

    if not validate_article(text):
        return None

    revision = page.get("revisions", [{}])[0]

    return {
        "record_id": str(uuid.uuid4()),
        "text": text,
        "source_name": "Egyptian Wikipedia",
        "source_url": page.get("fullurl"),
        "source_type": "wiki_article",
        "page_id": page.get("pageid"),
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
    """Write records as UTF-8 JSON Lines."""

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with OUTPUT_FILE.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(
                json.dumps(record, ensure_ascii=False) + "\n"
            )


def main():

    print("Starting Egyptian Wikipedia scraper...")

    titles = get_article_titles(MAX_ARTICLES)

    print(f"Retrieved {len(titles)} article titles.")

    records = []

    for start in range(0, len(titles), BATCH_SIZE):

        batch = titles[start:start + BATCH_SIZE]

        try:
            pages = fetch_articles(batch)

            for page in pages:
                record = create_record(page)

                if record:
                    records.append(record)

            print(
                f"Processed {min(start + BATCH_SIZE, len(titles))}"
                f"/{len(titles)} articles."
            )

        except requests.RequestException as error:
            print(f"Batch failed: {error}")

        time.sleep(REQUEST_DELAY)

    save_jsonl(records)

    print("\nScraping completed.")
    print(f"Valid records: {len(records)}")
    print(f"Output file: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
