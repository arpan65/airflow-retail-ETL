

# 🛍️ Retail E-commerce Analytics Pipeline with Apache Airflow, AWS, and PostgreSQL

This project builds an end-to-end data pipeline to process and analyze e-commerce event data. It uses **Apache Airflow 3.0** (asset-based DAGs), **PostgreSQL**, **AWS S3**, and **Glue Crawlers** to orchestrate ingestion, transformation, aggregation, and catalog updates for downstream analytics.

---

## 📁 Folder Structure

```
.
├── aws/                    # AWS-related setup/configs (e.g., credentials, S3)
├── config/                 # Optional: Airflow or pipeline configurations
├── dags/
│   ├── retail_pipeline_assets.py   # Asset-based DAG for raw to mart processing
│   └── retail_pipeline.py          # Traditional DAG for mart aggregation and Glue catalog update
├── data/                   # (Optional) Raw CSVs or staging data
├── logs/                   # Airflow logs
├── plugins/                # Custom Airflow plugins (if needed)
├── scripts/
│   └── table_creation.sql  # SQL scripts to create staging, fact, and dimension tables
├── docker-compose.yaml     # Local Airflow deployment with Docker
└── .venv/                  # Python virtual environment
```

---

## 📌 Pipeline Overview

### 1. **Staging and Modeling (`retail_pipeline_assets.py`)**

* Ingests raw e-commerce events (CSV) from S3 (partitioned by day).
* Loads data into:

  * `staging_events` (raw)
  * `dim_product`, `dim_user`, etc. (dimensions)
  * `fact_events` (fact table with enriched events)
* Supports incremental loads.

### 2. **Aggregations and Mart (`retail_pipeline.py`)**

* Computes daily KPIs: revenue, funnel conversion rates, top categories.
* Writes daily aggregates as **Parquet** to `s3://airflow-retail-mart/`.
* Triggers a **Glue Crawler** to update the AWS Glue Data Catalog for Athena/Redshift Spectrum querying.

---

## ⚙️ Tech Stack

| Component              | Purpose                                     |
| ---------------------- | ------------------------------------------- |
| **Apache Airflow 3.0** | Orchestration using tasks/assets/DAGs       |
| **PostgreSQL**         | Data warehouse for staging, fact/dim models |
| **AWS S3**             | Source and sink for CSVs and Parquet data   |
| **Glue Crawler**       | Catalogs Parquet data for Athena access     |
| **DuckDB/Pandas**      | Optional: Lightweight in-memory transforms  |
| **Docker Compose**     | Local Airflow setup with PostgreSQL         |

---

## 🚀 Quick Start (Local Deployment)

1. **Clone the repo**:

   ```bash
   git clone https://github.com/yourusername/retail-pipeline.git
   cd retail-pipeline
   ```

2. **Start Airflow with Docker Compose**:

   ```bash
   docker-compose up --build
   ```

3. **Access Airflow UI**:
   [http://localhost:8080](http://localhost:8080)

4. **Load tables**:
   Use the SQL in `scripts/table_creation.sql` to initialize your database.

---

## 📊 Key Metrics Tracked

* Total daily revenue
* Daily active users
* Views → Carts → Purchases funnel (conversion rates)
* Top categories/brands by revenue

---

## 🔐 AWS Setup Tips

* S3 buckets required:

  * `airflow-retail-stage` (raw CSVs in `month/YYYY-MM-DD/event.csv`)
  * `airflow-retail-mart` (aggregates in `aggregates/<metric>/dt=YYYY-MM-DD/`)
* Glue Crawler: `retail-parquet-crawler` should be pre-configured.
* Use an Airflow AWS connection named `aws`.

---

## ✅ To-Do / Enhancements

* Add unit tests for transformations
* Automate schema evolution with Glue
* Connect to AWS Athena for querying marts
* Add data quality checks with Airflow Sensors or Great Expectations

---

## 📄 License

MIT License. Free to use for learning and portfolio purposes.