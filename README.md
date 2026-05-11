**🏥 AnbarOne — Healthcare Data Pipeline with Apache Airflow & PySpark

![alt text](https://github.com/srour-topG/AnbarOne/blob/main/anbarOne.drawio.png?raw=true)

A modern healthcare ETL pipeline built using Apache Airflow, PySpark, and Docker following the Medallion Architecture (Bronze → Silver → Gold).

The project processes healthcare datasets including:

Patients data
Laboratory time-series
Vital signs time-series

and transforms them into clean analytical layers ready for downstream analytics and machine learning.

📌 Architecture
                +------------------+
                |   CSV Datasets   |
                +------------------+
                          |
                          v
                +------------------+
                |   Bronze Layer   |
                | Raw Parquet Data |
                +------------------+
                          |
                          v
                +------------------+
                |   Silver Layer   |
                | Cleaned & Valid  |
                +------------------+
                          |
                          v
                +------------------+
                |    Gold Layer    |
                | Analytics Ready  |
                +------------------+
🚀 Technologies Used
Python 3.8+
Apache Airflow
Apache Spark / PySpark
Docker & Docker Compose
Parquet
PostgreSQL JDBC Driver
📂 Project Structure
AnbarOne
├── dags/
│   └── anbar_pipeline.py       # Airflow DAG
│
├── datasets/
│   ├── patients.csv
│   ├── labs_timeseries.csv
│   └── vitals_timeseries.csv
│
├── Ingestion/
│   ├── bronze.py               # Raw ingestion
│   ├── silver.py               # Data cleaning & validation
│   ├── gold.py                 # Business-ready transformations
│   └── config.py               # Spark session configuration
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── README.md
└── postgresql-42.7.3.jar
⚙️ Pipeline Layers
🥉 Bronze Layer

Raw ingestion layer.

Responsibilities
Read CSV datasets
Infer schema automatically
Store raw data as Parquet
Preserve source structure
Input
datasets/*.csv
Output
Ingestion/bronze/
Run
python Ingestion/bronze.py
🥈 Silver Layer

Data cleaning and validation layer.

Responsibilities
Patients
Remove duplicates
Handle null values
Standardize gender values
Validate:
age
risk score
admission type
binary columns
Labs
Remove duplicate time records
Validate:
lactate
WBC count
creatinine
hemoglobin
sepsis risk score
Vitals
Clean oxygen device names
Validate:
heart rate
respiratory rate
SpO2
temperature
blood pressure
mobility score
Output
Ingestion/silver/
Run
python Ingestion/silver.py
🥇 Gold Layer

Business-ready analytics layer.

Responsibilities
Feature engineering
KPI generation
Aggregated healthcare metrics
ML-ready datasets
Output
Ingestion/gold/
Run
python Ingestion/gold.py
🌪 Apache Airflow DAG

The pipeline is orchestrated using Apache Airflow.

DAG Flow
bronze_layer
      ↓
silver_layer
      ↓
 gold_layer
DAG File
dags/anbar_pipeline.py
Trigger DAG

From Airflow UI:

http://localhost:8080
🐳 Docker Setup
Build Containers
docker-compose build
Start Services
docker-compose up -d
Stop Services
docker-compose down
📦 Install Dependencies (Without Docker)
pip install -r requirements.txt
🔧 Spark Configuration

Spark session is configured inside:

Ingestion/config.py

Example:

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("AnbarOne")
    .getOrCreate()
)
▶️ Running the Pipeline Manually
Step 1 — Bronze
python Ingestion/bronze.py
Step 2 — Silver
python Ingestion/silver.py
Step 3 — Gold
python Ingestion/gold.py
📊 Example Data Quality Rules
Dataset	Validation
Patients	Age > 0
Patients	Gender ∈ (M, F)
Labs	WBC Count > 0
Labs	Sepsis Risk Score between 0 and 1
Vitals	Heart Rate between 20 and 250
Vitals	Temperature between 30°C and 45°C
Vitals	SpO2 between 0 and 100
🧠 Medallion Architecture Benefits
Better data reliability
Incremental transformation
Easier debugging
Cleaner analytics
ML-ready pipelines
Scalable ETL design
📈 Future Improvements
Add unit testing
Add Great Expectations validation
Store Gold layer in PostgreSQL
Add dbt transformations
Add monitoring & alerting
CI/CD integration
Data lineage tracking
🛠 Example Airflow Task
bronze_task >> silver_task >> gold_task
📋 Requirements

Example dependencies:

apache-airflow
pyspark
pandas
pyarrow
🔥 Example Output
✅ patients start
✅ patients end

✅ Labs Started
✅ Labs End

✅ Vitals Start
✅ Vitals End

🔴 spark Stopped
👨‍💻 Author

Anbar

Healthcare Data Engineering Project using:

Apache Airflow
PySpark
Docker
Medallion Architecture
📄 License

This project is open-source and available under the MIT License.

By messaging ChatGPT, an AI chatbot, you agree to our Terms and have read our Privacy Policy. See Cookie Preferences.
Don't share sensitive info. Chats may be reviewed and used to train our models. Learn more
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
