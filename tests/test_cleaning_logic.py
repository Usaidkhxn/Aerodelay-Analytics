# tests/test_cleaning_logic.py

from flight_pipeline.db.mongo import get_database

def test_delay_flag_logic():
    db = get_database()
    sample = db.clean_flights.find_one(
        {"arrival_delay": {"$ne": None}}
    )

    assert sample is not None, "No sample flight found"

    if sample["arrival_delay"] > 0:
        assert sample["is_delayed"] is True
    else:
        assert sample["is_delayed"] is False


def test_cancelled_flights_consistency():
    db = get_database()
    sample = db.clean_flights.find_one()

    assert isinstance(sample["is_cancelled"], bool)
