import streamlit as st
import pandas as pd
from pymongo import MongoClient
import numpy as np


# MongoDB Connection
@st.cache_resource
def get_db():
    client = MongoClient(
        "mongodb://localhost:27017/?directConnection=true",
        serverSelectionTimeoutMS=5000
    )
    return client["flight_delay_db"]

db = get_db()

st.set_page_config(
    page_title="Flight Delay Big Data Analytics",
    layout="wide",
)

st.title("✈️ Flight Delay Big Data Analytics Dashboard")


def load_df(collection, limit=None):
    cursor = collection.find({}, {"_id": 0})
    if limit:
        cursor = cursor.limit(limit)
    return pd.DataFrame(list(cursor))

#  Daily Flight Volume vs Delays
st.header(" Daily Flight Volume vs Delays")

daily = load_df(db.agg_daily_flight_summary)
daily["flight_date"] = pd.to_datetime(daily["flight_date"])

col1, col2 = st.columns(2)

with col1:
    st.subheader("Total Flights per Day")
    st.line_chart(daily.set_index("flight_date")["total_flights"])

with col2:
    st.subheader("Delayed Flights per Day")
    st.line_chart(daily.set_index("flight_date")["delayed_flights"])

st.markdown(
    "**Insight:** Rising delay counts alongside stable flight volume indicate operational inefficiencies rather than congestion alone."
)

#  Worst Delay Days (Derived Metric)
st.header(" Worst Delay Days")

daily["delay_pct"] = (
    daily["delayed_flights"] / daily["total_flights"] * 100
)

worst_days = daily.sort_values("delay_pct", ascending=False).head(10)

st.bar_chart(
    worst_days.set_index("flight_date")["delay_pct"]
)

st.markdown(
    "**Insight:** Identifies extreme disruption days for weather, staffing, or system failure analysis."
)

import plotly.express as px

# Interactive Box Plot (Plotly)
st.header("📦 Arrival Delay Distribution by Airline")

clean = load_df(db.clean_flights)

# Keep only delayed flights
clean = clean[clean["arrival_delay"] > 0]

# Cap extreme outliers at 99th percentile
cap = clean["arrival_delay"].quantile(0.99)
clean["arrival_delay_capped"] = clean["arrival_delay"].clip(upper=cap)

# Top airlines by volume
top_airlines = (
    clean["airline"]
    .value_counts()
    .head(8)
    .index
)

plot_df = clean[clean["airline"].isin(top_airlines)]

fig = px.box(
    plot_df,
    x="airline",
    y="arrival_delay_capped",
    color="airline",
    points="outliers",   # interactive outliers
    title="Arrival Delay Distribution by Airline",
    labels={
        "airline": "Airline",
        "arrival_delay_capped": "Arrival Delay (minutes)"
    }
)

fig.update_layout(
    showlegend=False,
    height=500,
    margin=dict(l=40, r=40, t=60, b=40),
)

st.plotly_chart(fig, width="stretch")

st.markdown(
    """
**Why this matters:**  
This visualization highlights delay variability and extreme events across airlines.
Wide boxes and long whiskers indicate inconsistent operations, while compact boxes
suggest more reliable performance.
"""
)


#  Arrival Delay Severity Distribution
st.header(" Arrival Delay Severity Distribution")

# Sample for performance (still millions underneath)
clean_sample = load_df(db.clean_flights, limit=200_000)

bins = [-120, -15, 0, 15, 30, 60, 120, 300]
labels = [
    "Very Early",
    "Early",
    "On-Time",
    "Minor Delay",
    "Moderate Delay",
    "Severe Delay",
    "Extreme Delay",
]

clean_sample["delay_bucket"] = pd.cut(
    clean_sample["arrival_delay"],
    bins=bins,
    labels=labels,
)

severity_counts = clean_sample["delay_bucket"].value_counts().sort_index()

st.bar_chart(severity_counts)

st.markdown(
    "**Insight:** Most delays are small, but long-tail severe delays drive passenger dissatisfaction."
)

#  Heatmap: Airline × Day of Week Delay %
st.header(" Airline vs Day-of-Week Delay Heatmap")

clean_sample["dow"] = clean_sample["flight_date"].dt.day_name()

heatmap_df = (
    clean_sample.groupby(["airline", "dow"])
    .agg(
        total_flights=("is_delayed", "count"),
        delayed=("is_delayed", "sum"),
    )
    .reset_index()
)

heatmap_df["delay_pct"] = (
    heatmap_df["delayed"] / heatmap_df["total_flights"] * 100
)

pivot = heatmap_df.pivot(
    index="airline", columns="dow", values="delay_pct"
).fillna(0)

st.dataframe(
    pivot.style.background_gradient(cmap="Reds"),
    use_container_width=True,
)

st.markdown(
    "**Insight:** Reveals systematic operational weaknesses by airline and weekday."
)

#  Airline Reliability Comparison
st.header(" Airline Reliability Comparison")

airline = load_df(db.agg_airline_performance)

airline_renamed = airline.rename(
    columns={
        "pct_delayed": "Delay Percentage (%)",
        "pct_cancelled": "Cancellation Percentage (%)",
        "avg_arrival_delay": "Avg Arrival Delay (min)",
        "total_flights": "Total Flights",
        "airline": "Airline",
    }
)

st.bar_chart(
    airline_renamed.set_index("Airline")["Delay Percentage (%)"]
)

st.markdown(
    "**Insight:** Compares airline operational performance across millions of flights."
)


# Delay vs Cancellation Tradeoff (Scatter)
st.header(" Delay vs Cancellation Tradeoff")

st.scatter_chart(
    airline_renamed,
    x="Delay Percentage (%)",
    y="Cancellation Percentage (%)",
    size="Total Flights",
)

st.markdown(
    "**Insight:** Airlines strategically choose between delaying flights or canceling them."
)

# High-Risk Airports
st.header(" High-Risk Airports by Delay Probability")

airport = load_df(db.agg_airport_delay_stats)

airport_renamed = airport.rename(
    columns={
        "origin_airport": "Airport",
        "pct_delayed": "Delay Percentage (%)",
        "avg_departure_delay": "Avg Departure Delay (min)",
        "total_departures": "Total Departures",
    }
)

airport_top = airport_renamed.sort_values(
    "Delay Percentage (%)", ascending=False
).head(15)

st.bar_chart(
    airport_top.set_index("Airport")["Delay Percentage (%)"]
)

st.markdown(
    "**Insight:** Identifies infrastructure bottlenecks with outsized national impact."
)



