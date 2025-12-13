import logging
from pathlib import Path
from typing import Iterator

import pandas as pd
from pymongo.collection import Collection

from flight_pipeline.config.settings import mongo_settings
from flight_pipeline.db.mongo import get_database
from flight_pipeline.logging_config import setup_logging


RAW_DATA_DIR = Path("data/raw")
FLIGHT_CHUNK_SIZE = 100_000


def read_csv_in_chunks(path: Path, chunk_size: int) -> Iterator[pd.DataFrame]:
    """Yield DataFrames from a CSV file in chunks."""
    return pd.read_csv(path, chunksize=chunk_size)


def ingest_small_csv(path: Path, collection: Collection) -> None:
    """Ingest small CSV files (airlines, airports) in one shot."""
    logging.info(f"Ingesting {path.name}")
    df = pd.read_csv(path)
    records = df.to_dict(orient="records")

    if records:
        collection.insert_many(records)

    logging.info(f"Inserted {len(records)} records into {collection.name}")


def ingest_large_csv(path: Path, collection: Collection) -> None:
    """Ingest large CSV files (flights) using chunked ingestion."""
    logging.info(f"Starting chunked ingestion for {path.name}")

    total_inserted = 0
    for i, chunk in enumerate(read_csv_in_chunks(path, FLIGHT_CHUNK_SIZE), start=1):
        records = chunk.to_dict(orient="records")
        collection.insert_many(records)

        total_inserted += len(records)
        logging.info(
            f"{path.name} | chunk {i} | inserted {len(records)} | total {total_inserted}"
        )

    logging.info(f"Finished ingestion for {path.name} | total rows: {total_inserted}")


def run_raw_ingestion() -> None:
    setup_logging()
    db = get_database()

    # Collections
    raw_flights = db[mongo_settings.raw_flights]
    raw_airlines = db[mongo_settings.raw_airlines]
    raw_airports = db[mongo_settings.raw_airports]

    # Clear existing data (safe for re-runs)
    raw_flights.delete_many({})
    raw_airlines.delete_many({})
    raw_airports.delete_many({})

    ingest_small_csv(RAW_DATA_DIR / "airlines.csv", raw_airlines)
    ingest_small_csv(RAW_DATA_DIR / "airports.csv", raw_airports)
    ingest_large_csv(RAW_DATA_DIR / "flights.csv", raw_flights)

    logging.info("Raw ingestion completed successfully")


if __name__ == "__main__":
    run_raw_ingestion()
