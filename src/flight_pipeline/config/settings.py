from pydantic import BaseModel


class MongoSettings(BaseModel):
    uri: str = "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"


    database: str = "flight_delay_db"

    # Raw collections
    raw_flights: str = "raw_flights"
    raw_airlines: str = "raw_airlines"
    raw_airports: str = "raw_airports"

    # Clean collections
    clean_flights: str = "clean_flights"

    # Aggregated collections
    agg_daily_summary: str = "agg_daily_flight_summary"
    agg_airline_perf: str = "agg_airline_performance"
    agg_airport_stats: str = "agg_airport_delay_stats"


mongo_settings = MongoSettings()
