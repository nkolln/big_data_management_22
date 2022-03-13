import random
from pyspark.sql import SparkSession
import pyspark.sql.functions as pf
from pyspark.sql import SparkSession, functions
import re


def stock_preprocess():
    spark = SparkSession.builder.getOrCreate()
    textFile = spark.read.text("file:/opt/data/MS1.txt")
    usa_filter = textFile.filter(textFile.value.contains("USA"))
    year_filter = usa_filter.filter(usa_filter.value.contains("/2016") | usa_filter.value.contains("/2017") | usa_filter.value.contains("/2018") | usa_filter.value.contains("/2019"))
    ids = year_filter.select(functions.substring_index(year_filter.value, ".", 1).alias("value"))
    ids_unique = ids.dropDuplicates()
    id_list = ids_unique.collect()

    final_filter = year_filter
    for i in range(8907):
        random_index = random.choice(range(len(id_list)))
        id_list.remove(id_list[random_index])

    for i in range(len(id_list)):
        if i == 0:
            final_filter = year_filter.filter(functions.substring_index(year_filter.value, '.', 1) == id_list[i]['value'])
        else:
            final_filter = final_filter.union(year_filter.filter(functions.substring_index(year_filter.value, '.', 1) == id_list[i]['value']))

    print(final_filter)
    return(final_filter)
    #final_filter.repartition(200).toPandas().to_csv("Data/test.csv")

def preprocess_weather()->None:
    #import data
    spark = SparkSession.builder.getOrCreate()
    df_raw = spark.read.option("header",True).csv("/WeatherEvents.csv")
    
    #reset dates to be days instead of exact measurements
    df_processed = df_raw.withColumn("daily_timestamp", pf.date_trunc("day", df_raw['StartTime(UTC)']))

    #converts data to pivot and aggregates. Need to do by type as showed many different weather systems
    pivot_df = df_processed.groupBy("daily_timestamp").pivot("County").agg(pf.max(pf.col("Precipitation(in)")))

    #Outputs the data
    print(pivot_df)
    return(pivot_df)
    #pivot_df.repartition(100).toPandas().to_csv('Data/weather.csv')

def time_align(stocks, weather):

    spark = SparkSession.builder.getOrCreate()
    #stocks = spark.read.text("filepath to filtered stock data")
    #weather = spark.read.csv("Data/weather.csv")

    stock_dates = stocks.select(functions.regexp_extract(stocks.value, "([0-9]{2}\\/[0-9]{2}\\/[0-9]{4})", 1)).dropDuplicates()
    stock_dates = stock_dates.collect()


    weather_aligned = weather.filter(functions.array_contains(weather.daily_timestamp, stock_dates))

    print(weather_aligned)
    return(stock_dates,weather_aligned)
    #weather_aligned.write.option("header",True).csv("file path")

def format_stock(data_stocks):
    spark = SparkSession.builder.getOrCreate()

    df =spark.createDataFrame(data_stocks,["stock","daily_timestamp","price",'volume'])
    df.groupBy("daily_timestamp").pivot("stock").agg(pf.avg(pf.col("price")))
    print(df)
    return(df)


if __name__ == "__main__":
    data_stock = stock_preprocess()
    data_weather = preprocess_weather()
    data_stock,data_weather = time_align(data_stock,data_weather)
    data_stock = format_stock(data_stock)
    print('done')