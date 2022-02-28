from pyspark.sql import SparkSession
import pyspark.sql.functions as pf
import functions as f
from pyspark.sql.functions import col, countDistinct

spark = SparkSession.builder.getOrCreate()
df_raw = spark.read.option("header",True).csv("Data/head.csv")

f.Stats.count_unique(df_raw,'County')

df_raw = df_raw.withColumn("daily_timestamp", pf.date_trunc("day", df_raw['StartTime(UTC)']))

pivot_df = df_raw.groupBy("daily_timestamp").pivot("County").agg(pf.first(pf.col("Type")),pf.first(pf.col("Severity")), pf.first(pf.col("Precipitation(in)")))

#pivot_df = df_raw.groupBy("StartTime(UTC)").pivot("County").agg(pf.first(pf.col("Type","Severity")))
pivot_df.toPandas().to_csv('Data/weather.csv')