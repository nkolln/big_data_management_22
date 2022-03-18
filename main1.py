
import pyspark.sql.functions as pf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.types import *

from pandas import DataFrame
from pyspark.mllib.stat import Statistics
from pyspark.sql.functions import col, countDistinct
import pyspark.sql.functions as pf
from pyspark.ml.feature import StringIndexer


class Stats:
    def count_unique_mult(data:DataFrame)->None:
        out = data.agg(*(countDistinct(col(c)).alias(c) for c in data.columns))
        out.show()

    def count_unique(data:DataFrame,column:str)->None:
        data.agg(countDistinct(col(column)).alias("count")).show()

    def summary_statistics_rdd(data)->None:
        summary = Statistics.colStats(data)
        print(summary.mean())  # a dense vector containing the mean value for each column
        print(summary.variance())  # column-wise variance
        print(summary.numNonzeros())  # number 

class Transform:
    def categorical_conversion(data:DataFrame,col:str)->DataFrame:
        data = data.sort(pf.asc(col))
        #data.show()
        indexer = StringIndexer(inputCol=col, outputCol="category"+str(col))
        data = indexer.fit(data).transform(data)
        return(data)


def preprocess_weather()->None:
    #import data
    spark = SparkSession.builder.getOrCreate()
    df_raw = spark.read.option("header",True).csv("/WeatherEvents.csv")
    
    #reset dates to be days instead of exact measurements
    df_processed = df_raw.withColumn("daily_timestamps", pf.date_trunc("day", df_raw['StartTime(UTC)']))
    df_processed = df_processed.withColumn("daily_timestamp", pf.to_date(pf.col("daily_timestamps"),"MM/dd/yyyy")).drop("daily_timestamps")
    #df_processed.filter(df_processed("daily_timestamp") == lit("2015-03-14"))


    #converts data to pivot and aggregates. Need to do by type as showed many different weather systems
    pivot_df = df_processed.groupBy("daily_timestamp").pivot("County").agg(pf.max(pf.col("Precipitation(in)")))

    #Outputs the data
    #print(type(pivot_df))
    #pivot_df.show()
    #print(pivot_df)
    return(pivot_df)
    #pivot_df.repartition(100).toPandas().to_csv('Data/weather.csv')

def time_align(stocks, weather):

    spark = SparkSession.builder.getOrCreate()
    #stocks = spark.read.text("filepath to filtered stock data")
    #weather = spark.read.csv("Data/weather.csv")

    #stock_dates = stocks.select(functions.regexp_extract(stocks.value, "([0-9]{2}\\/[0-9]{2}\\/[0-9]{4})", 1)).dropDuplicates()
    #stock_dates = stock_dates.toPandas()

    #print(stock_dates)
    #weather_aligned = weather.filter(weather.daily_timestamp.isin(stock_dates))
    #weather_aligned = weather.filter(functions.array_contains(weather.daily_timestamp, stock_dates))

    return(stocks.join(weather,stocks.daily_timestamp ==  weather.daily_timestamp,"inner"))

    #weather_aligned.write.option("header",True).csv("file path")

def stock_preprocess_new():
    spark = SparkSession.builder.getOrCreate()
    textFile = spark.read.option("header", "true")\
        .text("file:/opt/data/MS1.txt")

    df = textFile.withColumn("Stock", split(pf.col("value"), ",").getItem(0)).withColumn("Date", split(pf.col("value"), ",").getItem(1)).withColumn("Price", split(pf.col("value"), ",").getItem(2)).withColumn("Volume", split(pf.col("value"), ",").getItem(3)).drop("value")
    #df.show()
    df = df.filter(df.Stock.contains("USA"))
    df = df.filter(df.Date.contains("/2016") | df.Date.contains("/2017") | df.Date.contains("/2018") | df.Date.contains("/2019"))
    df = df.sample(fraction=0.01, seed=3)

    df = df.withColumn("daily_timestamp", pf.to_date(pf.col("Date"),"MM/dd/yyyy")).drop("Date")
    df = Transform.categorical_conversion(df,"Stock")
    df = df.groupBy("daily_timestamp").pivot("categoryStock").agg(pf.avg(pf.col("Price")))
    #print(df)
    return(df)
    #df.show()

    """
    spark = SparkSession.builder.getOrCreate()
    
    data = spark.read.option("header", "false").text("Data/MS1.txt")
    #textFile = spark.read.option("header", "false")\
    #    .text("Data/MS1.txt")
    data = data.rdd.map(lambda k: k.split(","))
    #df=spark.createDataFrame(
    #    data,schema)
    df = df.toDF("Stock","Data","Price","Volume")
    print(df)
    df.show()

    """
    #textFile = spark.read.option("header", "false")\
    #    .text("Data/MS1.txt")
        
    #textFile.show()
    #textFile.selectExpr("split(value, ',')").show(3, False)
    #print(textFile.printSchema())
    #split_file = textFile.rdd.map(lambda k: k.split(","))
    #df = split_file.toDF(",")
    #print(df)

if __name__ == "__main__":
    #preprocess_weather()
    data_stock = stock_preprocess_new()
    data_weather = preprocess_weather()
    data_comb = time_align(data_stock,data_weather)
    print(data_comb)
    print(data_comb.show())


    """
    data_weather = preprocess_weather()
    print(type(data_weather))
    #data_weather = spark.read.option("header",True).csv("Data/data.csv")
    data_stock = spark.read.option("header",True).csv("Data/test.csv")
    #data_stock.show()
    data_weather = time_align(data_stock,data_weather)
    data_stock = format_stock(data_stock)
    print('done')
    """