🏥 AnbarOne — Healthcare Data Pipeline with Apache Airflow & PySpark

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
