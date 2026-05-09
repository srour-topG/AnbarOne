from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName('AnbarOne')
    .master('local[*]')
    .getOrCreate()
)
