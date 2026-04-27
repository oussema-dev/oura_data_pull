# Oura API v2 Daily Data Pipeline

This project provides a robust, production-ready pipeline for pulling, processing, and monitoring daily data from the Oura API v2 for multiple riders.
It is designed to be **fault-tolerant, automated, and coach-aware**, ensuring reliable long-term data collection with minimal manual intervention.


## Features

* **Historical + Incremental Sync**

  * First run pulls data from **January 1, 2025 → today**
  * Subsequent runs:

    * Re-pull the **last synced day**
    * Continue forward to today

* **Multi-Rider Support**

  * Handles multiple riders via a centralized config
  * Each rider produces **one row per day**

* **Multi-Endpoint Aggregation**
  Combines data from:

  * Daily Activity
  * Readiness
  * Sleep
  * Stress
  * SpO₂

* **Smart Data Processing**

  * Deduplicates records
  * Avoids partial "today" data
  * Extracts advanced sleep metrics (HR, HRV stats)

* **Automated Scheduling**

  * Runs immediately on startup
  * Then runs daily at **4:00 AM** via APScheduler

* **Monitoring & Logging**

  * Full pipeline logging → `log_oura.txt`
  * Tracks **API calls per minute** in real time

* **Inactivity Alerts**

  * Detects riders with **≥3 days of missing data**
  * Automatically emails coaches


## Configuration

All runtime configuration lives in `config.py`.

### Required Variables

```python
RIDER_LIST = {
    "Rider Name": {
        "token": "OURA_API_TOKEN",
        "email": "rider@email.com",
        "coach": "Coach Name",
        "coach_email": "coach@email.com"
    }
}
```

Other required fields:

* `DEFAULT_START_DATE`
* `CSV_FILE_FULL`
* `OURA_LOG_FILE`

### Email Settings

Used for inactivity alerts:

* `EMAIL_SERVER`
* `EMAIL_PORT`
* `EMAIL_SENDER`
* `EMAIL_PASSWORD`
* `CC_EMAIL`

## Data Collected

The pipeline extracts a wide range of metrics, including:

* Activity (e.g., non-wear time, high activity)
* Readiness (temperature deviations)
* Sleep:

  * Duration, efficiency, latency
  * Heart rate (min, max, avg)
  * HRV (min, max, avg)
  * Time of lowest heart rate
* Stress & recovery
* Blood oxygen (SpO₂)


## Important Note

**For a full description of all generated and extracted columns, refer to:**

**`oura_data_descriptor.txt`**

## Reliability & Design Principles

* **Defensive Programming**

  * API errors do not break the pipeline
  * Invalid responses return safe defaults

* **Rate Limit Handling**

  * Automatic retry with backoff on `429`

* **Resilient to Missing Data**

  * Partial/malformed responses are safely ignored

* **Idempotent Runs**

  * Re-pulling last day ensures data correctness

## Dependencies

* pandas
* requests
* python-dateutil
* apscheduler

Install with:

```bash
pip install pandas requests python-dateutil apscheduler
```