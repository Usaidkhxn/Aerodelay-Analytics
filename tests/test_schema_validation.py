# tests/test_schema_validation.py

from flight_pipeline.db.mongo import get_database

REQUIRED_FIELDS = {
    "flight_date",
    "airline",
    "origin_airport",
    "destination_airport",
    "departure_delay",
    "arrival_delay",
    "is_delayed",
    "is_cancelled",
}

def test_clean_flights_schema():
    db = get_database()
    doc = db.clean_flights.find_one()

    assert doc is not None, "clean_flights collection is empty"

    missing = REQUIRED_FIELDS - set(doc.keys())
    assert not missing, f"Missing required fields: {missing}"
