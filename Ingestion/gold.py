from config import spark
from pyspark.sql.functions import (
    col,
    avg,
    max,
    min
)

jdbc_url = "jdbc:postgresql://postgres:5432/warehouse"
properties = {
    "user": "spark",
    "password": "spark",
    "driver": "org.postgresql.Driver"
}


patients_silver = spark.read.parquet('Ingestion/silver/patients')
labs_silver = spark.read.parquet('Ingestion/silver/labs')
vitals_silver = spark.read.parquet('Ingestion/silver/vitals')


fact_patient_monitoring = (
    patients_silver

    .join(
        labs_silver,
        'patient_id'
    )

    .join(
        vitals_silver,
        ['patient_id', 'hour_from_admission']
    )

    .groupBy(
        'patient_id',
        'gender',
        'admission_type'
    )

    .agg(

        avg('heart_rate')
            .alias('avg_heart_rate'),

        avg('spo2_pct')
            .alias('avg_spo2'),

        avg('temperature_c')
            .alias('avg_temperature'),

        max('sepsis_risk_score')
            .alias('max_sepsis_risk'),

        avg('wbc_count')
            .alias('avg_wbc'),

        max('lactate')
            .alias('max_lactate'),

        max('creatinine')
            .alias('max_creatinine')
    )
)

fact_patient_monitoring.write.mode('overwrite').jdbc(
    jdbc_url,
    table='fact_patient_monitoring',
    properties=properties
)

print('✅ Fact Table Loaded')

dim_patient = (
    patients_silver.select(
        'patient_id',
        'age',
        'gender',
        'comorbidity_index',
        'admission_type',
        'baseline_risk_score'
    )
)

dim_patient.write.mode('overwrite').jdbc(
    jdbc_url,
    table='dim_patient',
    properties=properties
)

print('✅ Dim Patient Loaded')


dim_time = (
    vitals_silver.select(
        'hour_from_admission'
    ).distinct()
)

dim_time.write.mode('overwrite').jdbc(
    jdbc_url,
    table='dim_time',
    properties=properties
)

print('✅ Dim Time Loaded')



spark.stop()
print('🔴 spark Stopped')