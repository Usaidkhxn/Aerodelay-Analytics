import logging

from flight_pipeline.config.settings import mongo_settings
from flight_pipeline.db.mongo import get_database
from flight_pipeline.logging_config import setup_logging


def aggregate_daily_summary(db):
    logging.info("Building daily flight summary")

    pipeline = [
        {
            "$group": {
                "_id": "$flight_date",
                "total_flights": {"$sum": 1},
                "delayed_flights": {
                    "$sum": {"$cond": ["$is_delayed", 1, 0]}
                },
                "cancelled_flights": {
                    "$sum": {"$cond": ["$is_cancelled", 1, 0]}
                },
                "avg_arrival_delay": {"$avg": "$arrival_delay"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "flight_date": "$_id",
                "total_flights": 1,
                "delayed_flights": 1,
                "cancelled_flights": 1,
                "avg_arrival_delay": {"$round": ["$avg_arrival_delay", 2]},
            }
        },
    ]

    results = list(db.clean_flights.aggregate(pipeline))
    db[mongo_settings.agg_daily_summary].delete_many({})
    if results:
        db[mongo_settings.agg_daily_summary].insert_many(results)

    logging.info(f"Daily summary rows: {len(results)}")


def aggregate_airline_performance(db):
    logging.info("Building airline performance summary")

    pipeline = [
        {
            "$group": {
                "_id": "$airline",
                "total_flights": {"$sum": 1},
                "delayed_flights": {
                    "$sum": {"$cond": ["$is_delayed", 1, 0]}
                },
                "cancelled_flights": {
                    "$sum": {"$cond": ["$is_cancelled", 1, 0]}
                },
                "avg_arrival_delay": {"$avg": "$arrival_delay"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "airline": "$_id",
                "total_flights": 1,
                "pct_delayed": {
                    "$round": [
                        {
                            "$multiply": [
                                {"$divide": ["$delayed_flights", "$total_flights"]},
                                100,
                            ]
                        },
                        2,
                    ]
                },
                "pct_cancelled": {
                    "$round": [
                        {
                            "$multiply": [
                                {"$divide": ["$cancelled_flights", "$total_flights"]},
                                100,
                            ]
                        },
                        2,
                    ]
                },
                "avg_arrival_delay": {"$round": ["$avg_arrival_delay", 2]},
            }
        },
        {"$sort": {"pct_delayed": -1}},
    ]

    results = list(db.clean_flights.aggregate(pipeline))
    db[mongo_settings.agg_airline_perf].delete_many({})
    if results:
        db[mongo_settings.agg_airline_perf].insert_many(results)

    logging.info(f"Airline performance rows: {len(results)}")


def aggregate_airport_stats(db):
    logging.info("Building airport delay statistics")

    pipeline = [
        {
            "$group": {
                "_id": "$origin_airport",
                "total_departures": {"$sum": 1},
                "delayed_departures": {
                    "$sum": {"$cond": ["$is_delayed", 1, 0]}
                },
                "avg_departure_delay": {"$avg": "$departure_delay"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "origin_airport": "$_id",
                "total_departures": 1,
                "pct_delayed": {
                    "$round": [
                        {
                            "$multiply": [
                                {
                                    "$divide": [
                                        "$delayed_departures",
                                        "$total_departures",
                                    ]
                                },
                                100,
                            ]
                        },
                        2,
                    ]
                },
                "avg_departure_delay": {"$round": ["$avg_departure_delay", 2]},
            }
        },
        {"$sort": {"pct_delayed": -1}},
    ]

    results = list(db.clean_flights.aggregate(pipeline))
    db[mongo_settings.agg_airport_stats].delete_many({})
    if results:
        db[mongo_settings.agg_airport_stats].insert_many(results)

    logging.info(f"Airport stats rows: {len(results)}")


def run_aggregations():
    setup_logging()
    db = get_database()

    aggregate_daily_summary(db)
    aggregate_airline_performance(db)
    aggregate_airport_stats(db)

    logging.info("Aggregated layer completed successfully")


if __name__ == "__main__":
    run_aggregations()
