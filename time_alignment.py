from pyspark.sql import SparkSession, functions
import random
import pandas
import re

spark = SparkSession.builder.getOrCreate()
stocks = spark.read.text("filepath to filtered stock data")
weather = spark.read.csv("Data/weather.csv")

stock_dates = stocks.select(functions.regexp_extract(stocks.value, "([0-9]{2}\\/[0-9]{2}\\/[0-9]{4})", 1)).dropDuplicates()
stock_dates = stock_dates.collect()


weather_aligned = weather.filter(functions.array_contains(weather.daily_timestamp, stock_dates))

weather_aligned.write.option("header",True).csv("file path")
