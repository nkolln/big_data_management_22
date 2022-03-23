from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as pf
import functions as f
from pyspark.sql.functions import col
import sys

def preprocess_weather_different_categories()->None:
    #import data
    spark = SparkSession.builder.getOrCreate()
    df_raw = spark.read.option("header",True).csv("Data/WeatherEvents_Jan2016-Dec2021.csv")
    #df_raw.select(col("_c0").alias("id"))

    #encodes the Severity to Categorical (For now 0 = "Light", 1 = "Severe", 2 = "Moderate")
    df_raw = f.Transform.categorical_conversion(df_raw,'Severity')

    #encodes the Type to Categorical
    df_raw = f.Transform.categorical_conversion(df_raw,'Type')
    
    #reset dates to be hours instead of exact measurements
    df_processed = df_raw.withColumn("daily_timestamp", pf.date_trunc("day", df_raw['StartTime(UTC)']))

    #converts data to pivot and aggregates. Need to do by type as showed many different weather systems
    pivot_df = df_processed.groupBy("categoryType","daily_timestamp","County").agg(pf.first(pf.col("categorySeverity")).alias("categorySeverity"), pf.avg(pf.col("Precipitation(in)")).alias("avgPrecipitation"))
    
    #Converts each different type to an appropiate in line column
    #pivot_df = pivot_df.groupBy("daily_timestamp").pivot("categoryType").agg(pf.first(pf.col("categoryType")).alias("categoryType"),pf.first(pf.col("categorySeverity")).alias("categorySeverity"),pf.avg(pf.col("avgPrecipitation")).alias("avgPrecipitation"))
    #pivot_df.show()
    pivot_df = pivot_df.groupBy("daily_timestamp").pivot("County").agg(pf.first(pf.col("categoryType")).alias("categoryType"),pf.first(pf.col("categorySeverity")).alias("categorySeverity"),pf.avg(pf.col("avgPrecipitation")).alias("avgPrecipitation"))
    pivot_df.show()

    #pivot_df.write.option("header", True).csv(r"Data1/out.csv")
    #pivot_df.write.format("parquet").save("weather.parquet")
    #pivot_df.write.format("csv").mode("overwrite").save("Data1/")
    pivot_df.repartition(100).toPandas().to_csv('Data/weather.csv')

def preprocess_weather()->None:
    #import data
    spark = SparkSession.builder.getOrCreate()
    df_raw = spark.read.option("header",True).csv("Data/WeatherEvents_Jan2016-Dec2021.csv")
    
    #reset dates to be days instead of exact measurements
    df_processed = df_raw.withColumn("daily_timestamp", pf.date_trunc("day", df_raw['StartTime(UTC)']))

    #converts data to pivot and aggregates. Need to do by type as showed many different weather systems
    pivot_df = df_processed.groupBy("daily_timestamp").pivot("County").agg(pf.max(pf.col("Precipitation(in)")))

    #Outputs the data
    pivot_df.repartition(100).toPandas().to_csv('Data/weather.csv')


if __name__ == "__main__":
    preprocess_weather()
    #preprocess_weather_different_categories()