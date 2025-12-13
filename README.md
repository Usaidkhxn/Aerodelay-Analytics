# ✈️ AeroDelay Analytics

## Overview

This project implements an **end-to-end distributed Big Data pipeline** to analyze U.S. flight delays using a **MongoDB replica set**, Python-based processing, and an interactive dashboard. The system ingests over **5.8 million flight records**, processes them through **Raw → Clean → Gold** layers, and exposes analytics-ready aggregates via **Streamlit**.

The project is designed to demonstrate real-world Big Data engineering concepts including distributed storage, scalable ingestion, schema validation, and database-native aggregations.

---

## Dataset

* **Source:** Kaggle – U.S. DOT Flight Delays
* **Files Used:**

  * `flights.csv` (≈ 5.8M rows)
  * `airlines.csv`
  * `airports.csv`
* **Format:** CSV
* **Key Fields:** flight date, airline, flight number, origin airport, destination airport, departure delay, arrival delay, cancellation flag

---

## Architecture & Setup

### Distributed Platform

* **Database:** MongoDB 6.0
* **Deployment:** Docker Compose
* **Cluster Type:** **3-node Replica Set**
* **Nodes:**

  * `mongo1` – Primary
  * `mongo2` – Secondary
  * `mongo3` – Secondary

The replica set provides:

* High availability
* Fault tolerance
* Distributed data replication

### Container Orchestration

* Docker Compose manages all MongoDB services
* Each node runs in its own container
* Inter-container communication uses a dedicated Docker network

---

## Architecture Diagram

## Architecture and Setup

The system is deployed using Docker Compose and uses a distributed MongoDB replica set.
Data flows through Raw, Clean, and Gold layers before being consumed by a Streamlit dashboard.

```mermaid
...diagram here...

```mermaid
flowchart TB
    D[CSV Dataset]

    subgraph Python[Python Processing Layer]
        direction TB
        R[Raw Ingestion]
        C[Clean Layer]
        G[Gold Aggregations]
        R --> C --> G
    end

    subgraph MongoRS[MongoDB Replica Set]
        direction LR
        M1[mongo1<br/>PRIMARY]
        M2[mongo2<br/>SECONDARY]
        M3[mongo3<br/>SECONDARY]
        M1 <--> M2
        M1 <--> M3
    end

    subgraph Dashboard[Visualization Layer]
        S[Streamlit Dashboard]
    end

    D --> R
    R --> M1
    M1 --> C
    C --> M1
    M1 --> G
    G --> M1
    M1 --> S
```

---

## Pipeline Layers

### 1. Raw Layer (Bronze)

* Ingests CSV files **as-is** into MongoDB
* Uses **chunked ingestion (100,000 rows per chunk)** for scalability
* Collections:

  * `raw_flights`
  * `raw_airlines`
  * `raw_airports`

**Row counts:**

* Flights: ~5.8M
* Airlines: 14
* Airports: ~322

---

### 2. Clean Layer (Silver)

* Processes a **1.5M representative sample** of flights

* Cleaning steps:

  * Handle missing delay values
  * Normalize airline and airport codes
  * Standardize flight dates
  * Derive delay and cancellation indicators
  * Validate schema using **Pydantic**
  * Remove duplicates using a composite key

* Output collection:

  * `clean_flights` (~1.45M rows)

---

### 3. Aggregated Layer (Gold)

Aggregations are computed **directly in MongoDB** using aggregation pipelines and materialized as collections:

1. **Daily Flight Summary** (`agg_daily_flight_summary`)

   * Total flights
   * Delayed flights
   * Cancelled flights
   * Average arrival delay

2. **Airline Performance** (`agg_airline_performance`)

   * % delayed
   * % cancelled
   * Average arrival delay

3. **Airport Delay Statistics** (`agg_airport_delay_stats`)

   * % delayed departures
   * Average departure delay

These collections are optimized for fast dashboard queries.

---

## Dashboard

* **Tool:** Streamlit
* **Data Source:** MongoDB Gold-layer collections
* **Visualizations:**

  1. Daily flight volume vs delayed flights (time series)
  2. Airline delay percentage (bar chart)
  3. Top airports by delay percentage (bar chart)

Run locally:

```bash
streamlit run dashboard/app.py
```

---

## Project Structure

```
flight-delay-bigdata-pipeline/
│── docker-compose.yml
│── README.md
│
├── data/
│   └── raw/
│       ├── flights.csv
│       ├── airlines.csv
│       └── airports.csv
│
├── src/flight_pipeline/
│   ├── config/
│   ├── db/
│   ├── models/
│   ├── pipeline/
│   └── logging_config.py
│
├── dashboard/
│   └── app.py
│
└── tests/
```

---

## Key Technologies

* MongoDB (Replica Set)
* Docker & Docker Compose
* Python (Pandas, PyMongo, Pydantic)
* Streamlit

---

## Summary

This project demonstrates a **production-style Big Data architecture** with distributed storage, scalable ingestion, schema validation, database-native analytics, and interactive visualization — all implemented using open-source tools and real-world data.

## Dataset

U.S. Domestic Flight Delay dataset (2015)
Source: Kaggle / U.S. Department of Transportation
link: https://www.kaggle.com/datasets/usdot/flight-delays
