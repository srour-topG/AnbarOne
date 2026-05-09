from config import spark

base_path = '/app/datasets/'

tables = {
    'patients':'patients.csv',
    'labs_timeseries':'labs_timeseries.csv',
    'vitals_timeseries':'vitals_timeseries.csv',
}

for name, file in tables.items():
    print(f'✅ {name} start')
    df = (
        spark.read
        .option('header',True)
        .option('inferschema',True)
        .csv(base_path+file)
    )
    df.write.mode('overwrite').parquet(f'Ingestion/bronze/{name}')
    print(f'✅ {name} end')

spark.stop()
print(f'🔴 spark Stopped')    