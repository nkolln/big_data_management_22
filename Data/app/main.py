
import pyspark.sql.functions as pf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.types import *

def preprocess_weather()->None:
    #import data
    spark = SparkSession.builder.getOrCreate()
    df_raw = spark.read.option("header",True).csv("Data/WeatherEvents.csv")
    
    #reset dates to be days instead of exact measurements
    df_processed = df_raw.withColumn("daily_timestamp", pf.date_trunc("day", df_raw['StartTime(UTC)']))

    #converts data to pivot and aggregates. Need to do by type as showed many different weather systems
    pivot_df = df_processed.groupBy("daily_timestamp").pivot("County").agg(pf.max(pf.col("Precipitation(in)")))

    #Outputs the data
    print(type(pivot_df))
    pivot_df.show()
    return(pivot_df)
    #pivot_df.repartition(100).toPandas().to_csv('Data/weather.csv')

def time_align(stocks, weather):

    spark = SparkSession.builder.getOrCreate()
    #stocks = spark.read.text("filepath to filtered stock data")
    #weather = spark.read.csv("Data/weather.csv")

    stock_dates = stocks.select(pf.functions.regexp_extract(stocks.value, "([0-9]{2}\\/[0-9]{2}\\/[0-9]{4})", 1)).dropDuplicates()
    stock_dates = stock_dates.toPandas()
    print(stock_dates)
    weather_aligned = weather.filter(weather.daily_timestamp.isin(stock_dates))
    #weather_aligned = weather.filter(functions.array_contains(weather.daily_timestamp, stock_dates))

    print(weather_aligned)
    return(weather_aligned)
    #weather_aligned.write.option("header",True).csv("file path")

def format_stock(data_stocks):
    spark = SparkSession.builder.getOrCreate()

    df =spark.createDataFrame(data_stocks,["stock","daily_timestamp","price",'volume'])
    df.groupBy("daily_timestamp").pivot("stock").agg(pf.avg(pf.col("price")))
    df.show()
    return(df)

def stock_preprocess_new():
    spark = SparkSession.builder.getOrCreate()
    textFile = spark.read.option("header", "true")\
        .text("file:/opt/data/MS1.txt")

    df = textFile.withColumn("Stock", split(pf.col("value"), ",").getItem(0)).withColumn("Date", split(pf.col("value"), ",").getItem(1)).withColumn("Price", split(pf.col("value"), ",").getItem(2)).withColumn("Volume", split(pf.col("value"), ",").getItem(3)).drop("value")
    #df.show()
    df = df.sample(fraction=0.01, seed=3)
    df = df.filter(df.Stock.contains("USA"))
    df = df.filter(df.Date.contains("/2016") | df.Date.contains("/2017") | df.Date.contains("/2018") | df.Date.contains("/2019"))
    
    #print(df.count())
    df = df.withColumn("daily_timestamp", pf.to_date(pf.col("Date"),"MM/dd/yyyy")).drop("Date")
    #df = f.Transform.categorical_conversion(df,"Stock")
    df = df.groupBy("daily_timestamp").pivot("Stock").agg(pf.avg(pf.col("Price")))
    print(df)
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