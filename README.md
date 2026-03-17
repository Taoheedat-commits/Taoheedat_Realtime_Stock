# Real-Time Stock Market Data Pipeline

## Overview

This project implements a modular data engineering pipeline designed to extract real-time stock market data from the Alpha Vantage API and prepare it for distributed processing and analytics.

The system demonstrates how modern data platforms integrate API ingestion, data streaming, distributed computation, and persistent storage using containerised infrastructure.

This project is built as a learning exercise in **data engineering architecture**, focusing on scalable pipelines and modular design.

---

## Architecture

The pipeline is designed to support a streaming data architecture using the following components:

- **Python Producer** – Extracts stock market data from the Alpha Vantage API
- **Apache Kafka** – Handles real-time data streaming
- **Apache Spark** – Performs distributed data processing
- **PostgreSQL** – Stores processed stock data
- **pgAdmin** – Database administration interface
- **Docker Compose** – Container orchestration



### producer/

Contains the Python producer pipeline responsible for extracting and preparing stock market data.

**config.py**

Handles:

- environment variable loading
- API configuration
- logging configuration

**extract.py**

Responsible for:

- connecting to the Alpha Vantage API
- retrieving time-series stock data
- extracting and structuring the JSON response

**main.py**

Pipeline entry point that:

- triggers API extraction
- processes returned records
- logs pipeline status

---

## Technologies Used

- Python
- Docker
- Docker Compose
- Apache Kafka
- Apache Spark
- PostgreSQL
- pgAdmin
- Alpha Vantage API

---

## Logging

The pipeline uses Python’s logging module to track pipeline activity.

Example output:




---

## Running the Pipeline

### 1. Activate virtual environment
venv\scripts\activate


### 2. Run the producer
python producer/main.py



---

## Running the Infrastructure Stack

The project includes a Docker Compose configuration that launches the full data infrastructure.

Services included:

- Spark Master
- Spark Worker
- Kafka Broker
- Kafka UI
- PostgreSQL
- pgAdmin

Start the infrastructure:
docker compose -d


Stop the stack:
docker compose down


---

## Environment Variables

Create a `.env` file in the project root.

Example:
