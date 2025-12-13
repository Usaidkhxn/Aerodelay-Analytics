import logging
from datetime import datetime
from typing import Dict, List

from pymongo import ASCENDING
from pymongo.collection import Collection
from pydantic import ValidationError

from flight_pipeline.config.settings import mongo_settings
from flight_pipeline.db.mongo import get_database
from flight_pipeline.logging_config import setup_logging
from flight_pipeline.models.clean import CleanFlight


BATCH_SIZE = 50_000
MAX_RECORDS = 1_500_000


def transform_raw_flight(doc: Dict) -> Dict | None:
    """Transform a raw flight document into a clean flight record."""
    try:
        is_cancelled = bool(doc.get("CANCELLED", 0))

        dep_delay = doc.get("DEPARTURE_DELAY")
        arr_delay = doc.get("ARRIVAL_DELAY")

        if not is_cancelled:
            dep_delay = int(dep_delay) if dep_delay is not None else 0
            arr_delay = int(arr_delay) if arr_delay is not None else 0

        flight_date = datetime(
            int(doc["YEAR"]),
            int(doc["MONTH"]),
            int(doc["DAY"]),
        )

        clean = CleanFlight(
    flight_date=flight_date,
    airline=str(doc["AIRLINE"]).upper().strip(),
    origin_airport=str(doc["ORIGIN_AIRPORT"]).upper().strip(),
    destination_airport=str(doc["DESTINATION_AIRPORT"]).upper().strip(),
    flight_number=str(doc.get("FLIGHT_NUMBER")),
    departure_delay=dep_delay,
    arrival_delay=arr_delay,
    is_delayed=(arr_delay is not None and arr_delay > 15),
    is_cancelled=is_cancelled,
)


        return clean.model_dump()

    except (KeyError, ValueError, ValidationError):
        return None


def run_clean_pipeline() -> None:
    setup_logging()
    db = get_database()

    raw = db[mongo_settings.raw_flights]
    clean = db[mongo_settings.clean_flights]

    # Clear for safe re-runs
    clean.delete_many({})

    cursor = raw.find({}, no_cursor_timeout=True).limit(MAX_RECORDS)

    buffer: List[Dict] = []
    processed = 0
    inserted = 0

    for doc in cursor:
        processed += 1
        cleaned = transform_raw_flight(doc)

        if cleaned:
            buffer.append(cleaned)

        if len(buffer) >= BATCH_SIZE:
            clean.insert_many(buffer)
            inserted += len(buffer)
            buffer.clear()

            logging.info(
                f"Processed {processed:,} | Inserted {inserted:,}"
            )

    if buffer:
        clean.insert_many(buffer)
        inserted += len(buffer)

    logging.info(
        f"Clean layer completed | processed {processed:,} | inserted {inserted:,}"
    )

    # Deduplication index
    clean.create_index(
    [
        ("flight_date", ASCENDING),
        ("airline", ASCENDING),
        ("flight_number", ASCENDING),
        ("origin_airport", ASCENDING),
        ("destination_airport", ASCENDING),
    ],
    unique=True,
    background=True,
)


    logging.info("Deduplication index created")


if __name__ == "__main__":
    run_clean_pipeline()
