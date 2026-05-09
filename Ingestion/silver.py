from config import spark
from pyspark.sql.functions import to_timestamp,col


patients_bronze = spark.read.parquet('Ingestion/bronze/patients')
labs_bronze = spark.read.parquet('Ingestion/bronze/labs')
vitals_bronze = spark.read.parquet('Ingestion/bronze/vitals')


print('✅ Patients Started')
patient_silver = (
    patients_bronze
    .dropDuplicates(['patient_id'])
    .filter(
        col('patient_id').isNotNull() &
        col('age').isNotNull() &
        col('gender').isNotNull()
    )
    .withColumn('gender', upper(trim(col('gender'))))
    .withColumn('admission_type', trim(col('admission_type')))
    
    .filter(
        col('age') > 0 &
        col('gender').isin('M','F') &
        col('comorbidity_index') >= 0 &
        col('baseline_risk_score').between(0,1) &
        col('los_hours') > 0 &
        col('deterioration_event').isin(0,1) &
        col('deterioration_within_12h_from_admission').isin(0,1) &
        col('deterioration_hour') >= -1 &
        col('admission_type').isin(
            'ED',
            'Elective',
            'Transfer'
        )
    )
)
patient_silver.write.mode('overwrite').parquet('Ingestion/silver/patients')

print('✅ Patients End')


print('✅ Labs Started')

lab_silver = (
    labs_bronze
    .dropDuplicates([
        'patient_id',
        'hour_from_admission'
    ])
    .filter(
        col('patient_id').isNotNull() &
        col('hour_from_admission').isNotNull()
    )
    .filter(
        col('hour_from_admission') >= 0 & 
        col('wbc_count') > 0 &
        col('lactate') >= 0 &
        col('creatinine') >= 0 &
        col('crp_level') >= 0 &
        col('hemoglobin') > 0 &
        col('sepsis_risk_score').between(0,1)
    )
)

lab_silver.write.mode('overwrite').parquet('Ingestion/silver/labs')

print('✅ Labs End')

print('✅ Vitals Start')
vitals_silver = (
    vitals_bronze
    .dropDuplicates([
        'patient_id',
        'hour_from_admission'
    ])
    .filter(
        col('patient_id').isNotNull() &
        col('heart_rate').isNotNull()
    )
    .withColumn(
        'oxygen_device',
        lower(trim(col('oxygen_device')))
    )
    # valid admission hour
    .filter(col('hour_from_admission') >= 0)

    # heart rate validation
    .filter(
        col('heart_rate').between(20,250)
    )

    # respiratory rate validation
    .filter(
        col('respiratory_rate').between(5,60)
    )

    # oxygen saturation
    .filter(
        col('spo2_pct').between(0,100)
    )

    # body temperature
    .filter(
        col('temperature_c').between(30,45)
    )

    # blood pressure
    .filter(col('systolic_bp') > 0)
    .filter(col('diastolic_bp') > 0)

    # oxygen flow
    .filter(col('oxygen_flow') >= 0)

    # mobility score
    .filter(
        col('mobility_score').between(0,5)
    )

    # nurse alert binary
    .filter(
        col('nurse_alert').isin(0,1)
    )
    
)
vitals_silver.write.mode('overwrite').parquet('Ingestion/silver/vitals')
print('✅ Vitals End')


spark.stop()
print('🔴 spark Stopped')