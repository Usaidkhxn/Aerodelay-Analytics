# tests/test_aggregation_outputs.py

from flight_pipeline.db.mongo import get_database

def test_daily_aggregation_exists():
    db = get_database()
    doc = db.agg_daily_flight_summary.find_one()

    assert doc is not None, "Daily aggregation missing"

    assert "total_flights" in doc
    assert "delayed_flights" in doc
    assert "flight_date" in doc


def test_airline_aggregation_ranges():
    db = get_database()
    doc = db.agg_airline_performance.find_one()

    assert doc is not None, "Airline aggregation missing"

    assert  ("pct_delayed" in doc), "pct_delayed missing"
    assert 0 <= doc["pct_delayed"] <= 100


def test_airport_delay_stats():
    db = get_database()
    doc = db.agg_airport_delay_stats.find_one()

    assert doc is not None, "Airport delay stats missing"

    assert 0 <= doc["pct_delayed"] <= 100
