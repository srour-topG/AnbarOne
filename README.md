**# 🏥 AnbarOne  
### Healthcare Data Engineering Pipeline with Apache Airflow & PySpark

<p align="center">
  <img src="https://github.com/srour-topG/AnbarOne/blob/main/anbarOne.drawio.png?raw=true" width="900"/>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.8+-blue?style=for-the-badge&logo=python"/>
  <img src="https://img.shields.io/badge/Apache-Airflow-red?style=for-the-badge&logo=apacheairflow"/>
  <img src="https://img.shields.io/badge/PySpark-Data%20Engineering-orange?style=for-the-badge&logo=apachespark"/>
  <img src="https://img.shields.io/badge/Docker-Containerized-blue?style=for-the-badge&logo=docker"/>
  <img src="https://img.shields.io/badge/Architecture-Medallion-success?style=for-the-badge"/>
</p>

---

# 📖 Overview

**AnbarOne** is a modern healthcare ETL pipeline designed using:

- Apache Airflow
- PySpark
- Docker
- Medallion Architecture

The pipeline processes healthcare datasets including:

- 👤 Patients Data
- 🧪 Laboratory Time-Series
- ❤️ Vital Signs Time-Series

and transforms them into scalable, analytics-ready datasets for:

- Business Intelligence
- Healthcare Analytics
- Machine Learning
- Data Warehousing

---

# 🏗️ Architecture

```text
                +----------------------+
                |    CSV Datasets      |
                +----------------------+
                           |
                           v
                +----------------------+
                |    Bronze Layer      |
                |   Raw Parquet Data   |
                +----------------------+
                           |
                           v
                +----------------------+
                |    Silver Layer      |
                | Cleaned & Validated  |
                +----------------------+
                           |
                           v
                +----------------------+
                |     Gold Layer       |
                | Analytics & ML Ready |
                +----------------------+
```

---

# ⚡ Tech Stack

| Technology | Purpose |
|---|---|
| Python | Core Programming |
| Apache Airflow | Workflow Orchestration |
| Apache Spark / PySpark | Distributed Data Processing |
| Docker | Containerization |
| Parquet | Optimized Data Storage |
| PostgreSQL JDBC | Database Connectivity |

---

# 📂 Project Structure

```bash
AnbarOne
├── dags/
│   └── anbar_pipeline.py
│
├── datasets/
│   ├── patients.csv
│   ├── labs_timeseries.csv
│   └── vitals_timeseries.csv
│
├── Ingestion/
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   └── config.py
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── README.md
└── postgresql-42.7.3.jar
```

---

# 🥉 Bronze Layer

## Responsibilities

- Read raw CSV datasets
- Infer schema automatically
- Convert data into Parquet
- Preserve original source structure

## Input

```bash
datasets/*.csv
```

## Output

```bash
Ingestion/bronze/
```

## Run

```bash
python Ingestion/bronze.py
```

---

# 🥈 Silver Layer

## Responsibilities

### 👤 Patients Dataset

- Remove duplicates
- Handle missing values
- Standardize gender values
- Validate:
  - Age
  - Risk Score
  - Admission Type
  - Binary Columns

### 🧪 Labs Dataset

- Remove duplicate timestamps
- Validate:
  - Lactate
  - WBC Count
  - Creatinine
  - Hemoglobin
  - Sepsis Risk Score

### ❤️ Vitals Dataset

- Clean oxygen device names
- Validate:
  - Heart Rate
  - Respiratory Rate
  - SpO2
  - Temperature
  - Blood Pressure
  - Mobility Score

## Output

```bash
Ingestion/silver/
```

## Run

```bash
python Ingestion/silver.py
```

---

# 🥇 Gold Layer

## Responsibilities

- Feature Engineering
- KPI Generation
- Healthcare Aggregations
- ML-ready Dataset Creation
- Business Analytics Preparation

## Output

```bash
Ingestion/gold/
```

## Run

```bash
python Ingestion/gold.py
```

---

# 🌪️ Apache Airflow Orchestration

The pipeline is orchestrated using **Apache Airflow**.

## DAG Flow

```text
bronze_layer
      ↓
silver_layer
      ↓
 gold_layer
```

## DAG File

```bash
dags/anbar_pipeline.py
```

## Airflow UI

```bash
http://localhost:8080
```

---

# 🐳 Docker Setup

## Build Containers

```bash
docker-compose build
```

## Start Services

```bash
docker-compose up -d
```

## Stop Services

```bash
docker-compose down
```

---

# ⚙️ Local Installation

## Install Dependencies

```bash
pip install -r requirements.txt
```

---

# 🔧 Spark Configuration

Located in:

```bash
Ingestion/config.py
```

Example:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("AnbarOne")
    .getOrCreate()
)
```

---

# ▶️ Running the Pipeline Manually

## Step 1 — Bronze Layer

```bash
python Ingestion/bronze.py
```

## Step 2 — Silver Layer

```bash
python Ingestion/silver.py
```

## Step 3 — Gold Layer

```bash
python Ingestion/gold.py
```

---

# 📊 Data Quality Rules

| Dataset | Validation Rule |
|---|---|
| Patients | Age > 0 |
| Patients | Gender ∈ (M, F) |
| Labs | WBC Count > 0 |
| Labs | Sepsis Risk Score ∈ [0,1] |
| Vitals | Heart Rate between 20 and 250 |
| Vitals | Temperature between 30°C and 45°C |
| Vitals | SpO2 between 0 and 100 |

---

# 🧠 Why Medallion Architecture?

✅ Better Data Reliability  
✅ Easier Debugging  
✅ Incremental Data Transformation  
✅ Scalable ETL Pipelines  
✅ Analytics-Ready Data  
✅ ML-Friendly Structure  

---

# 🔥 Example Pipeline Output

```bash
✅ Patients Started
✅ Patients Finished

✅ Labs Started
✅ Labs Finished

✅ Vitals Started
✅ Vitals Finished

🔴 Spark Session Stopped
```

---

# 🚀 Future Improvements

- [ ] Unit Testing
- [ ] Great Expectations Validation
- [ ] PostgreSQL Gold Storage
- [ ] dbt Transformations
- [ ] Monitoring & Alerting
- [ ] CI/CD Integration
- [ ] Data Lineage Tracking
- [ ] Kafka Streaming Support

---

# 🛠️ Example Airflow Dependency

```python
bronze_task >> silver_task >> gold_task
```

---

# 📦 Requirements

```txt
apache-airflow
pyspark
pandas
pyarrow
```

---

# 👨‍💻 Author

## Anbar

Healthcare Data Engineering Project built with:

- Apache Airflow
- PySpark
- Docker
- Medallion Architecture

---

# 📄 License

This project is licensed under the **MIT License**.
