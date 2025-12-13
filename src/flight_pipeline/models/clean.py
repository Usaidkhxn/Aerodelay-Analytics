from datetime import datetime
from pydantic import BaseModel, Field


class CleanFlight(BaseModel):
    flight_date: datetime
    airline: str
    origin_airport: str
    destination_airport: str
    flight_number: str

    departure_delay: int | None
    arrival_delay: int | None

    is_delayed: bool
    is_cancelled: bool
