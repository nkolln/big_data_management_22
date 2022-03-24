

from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.types import *

from pandas import DataFrame
from pyspark.mllib.stat import Statistics
from pyspark.sql.functions import col, countDistinct
import pyspark.sql.functions as pf
from pyspark.ml.feature import StringIndexer

import math
import json

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


    #converts data to pivot and aggregates. Need to do by type as showed many different weather systems
    df_processed = df_processed.withColumn("Precipitation(in)", df_processed["Precipitation(in)"].cast(IntegerType()))
    pivot_df = df_processed.groupBy("daily_timestamp").pivot("County").agg(pf.max(pf.col("Precipitation(in)")))
    pivot_df = pivot_df.fillna(0)

    lst_tmp = []
    for col in pivot_df.columns:
        strip_name = col.strip()
        strip_name = "".join(strip_name.split())
        strip_name = strip_name.replace('.','') 
        lst_tmp.append(strip_name) 
    pivot_df = pivot_df.toDF(*lst_tmp)

    return(pivot_df)

def time_align(stocks, weather):

    return(stocks.join(weather,stocks.daily_timestamp ==  weather.daily_timestamp,"inner"))

def stock_preprocess_new():
    spark = SparkSession.builder.getOrCreate()
    textFile = spark.read.option("header", "true")\
        .text("file:/opt/data/MS1.txt")

    df = textFile.withColumn("Stock", split(pf.col("value"), ",").getItem(0)).withColumn("Date", split(pf.col("value"), ",").getItem(1)).withColumn("Price", split(pf.col("value"), ",").getItem(2)).withColumn("Volume", split(pf.col("value"), ",").getItem(3)).drop("value")
    #df.show()
    df = df.filter(df.Stock.contains("USA"))
    df = df.filter(df.Date.contains("/2016") | df.Date.contains("/2017") | df.Date.contains("/2018") | df.Date.contains("/2019"))
    #df = df.sample(fraction=0.01, seed=3)
    df = df.limit(1250000)

    df = df.withColumn("daily_timestamp", pf.to_date(pf.col("Date"),"MM/dd/yyyy")).drop("Date")
    df = Transform.categorical_conversion(df,"Stock")
    df = df.groupBy("daily_timestamp").pivot("categoryStock").agg(pf.avg(pf.col("Volume")))
    df = df.fillna(0)
    lst_tmp = []
    for col in df.columns:
        strip_name = col.strip()
        strip_name = "".join(strip_name.split())
        strip_name = strip_name.replace('.0','')
        lst_tmp.append(strip_name)

    df = df.toDF(*lst_tmp)

    return(df)

def part3(data):
    spark = SparkSession.builder.getOrCreate()
    #print(data.show(10))
    #data = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("Data/medium_test.csv")
    data = data.drop("_c0", "daily_timestamp1", "daily_timestamp1212")
    #data.show()
    # data.show(n=10, truncate=False)
    ind_names = []
    col_names = data.schema.fieldNames()
    # stock_names = col_names[2:7]
    index_of_first_county = 0
    for i, value in enumerate(col_names):
        if value == "Accomack":
            ind_names = [str(j) for j in range(i)]
            # print("ind_names:", ind_names)
            index_of_first_county = i
            break

    county_names = col_names[index_of_first_county:]
    col_names = ind_names+county_names
    data = data.toDF(*col_names)
    # data.show(n=10, truncate=False)
    col_names = data.schema.fieldNames()
    stock_names = col_names[:index_of_first_county]
    county_names = col_names[index_of_first_county:]
    final_dict = {}


    result_index = 0
    for stock in stock_names:
        # stock = float(stock)
        # print(stock)
        # stock = str(stock)
        # print(stock)
        county_index = 0
        for i, county1 in enumerate(county_names):
            rest_of_the_counties = county_names[i+1:]

            for count2 in rest_of_the_counties:
                # min = data.withColumn("min", least(col(county1), col(count2)))
                # min_sq = min.select(pow("min", 2).alias("min_sq")).agg({'min_sq': 'sum'}).collect()[0][0]
                # min_sqrt = math.sqrt(min_sq)
                # print("min_sqrt: ", min_sqrt)

                    
                max = data.withColumn("max", pf.greatest(col(county1), col(count2)))
                max_sq = max.select((pf.pow(max["max"], 2)).alias("max_sq")).agg({'max_sq': 'sum'}).collect()[0][0]
                max_sqrt = math.sqrt(max_sq)
                # print("max_sqrt: ", max_sqrt)

                avg = data.withColumn("avg", (col(county1) + col(count2)) / pf.lit(2))
                avg_sq = avg.select(pf.pow(avg["avg"], 2).alias("avg_sq")).agg({'avg_sq': 'sum'}).collect()[0][0]
                avg_sqrt = math.sqrt(avg_sq)
                # print("avg_sqrt: ", avg_sqrt)

                stocks = data.select(col(stock).alias("stocks"))
                stocks_sq = stocks.select(pf.pow(stocks["stocks"], 2).alias("stocks_sq")).agg({'stocks_sq': 'sum'}).collect()[0][0]
                stocks_sqrt = math.sqrt(stocks_sq)
                # print("stocks_sqrt: ", stocks_sqrt)

                # min_cos = min.select((col(stock) * col("min")).alias("min_mult")).agg({'min_mult': 'sum'}).collect()[0][0] / (
                #             stocks_sqrt * min_sqrt)
                max_cos = max.select((col(stock) * col("max")).alias("max_mult")).agg({'max_mult': 'sum'}).collect()[0][0] / (
                            stocks_sqrt * max_sqrt)
                avg_cos = avg.select((col(stock) * col("avg")).alias("avg_mult")).agg({'avg_mult': 'sum'}).collect()[0][0] / (
                            stocks_sqrt * avg_sqrt)

                result = {"stock": stock,
                        "county1": county1,
                        "county2": count2,
                        # "min_cos": min_cos,
                        "max_cos": max_cos,
                        "avg_cos": avg_cos
                        }
                final_dict.update({f"result{stock}-{county1}": result})


            county_index += 1

        result_index += 1
        #with open('results_part3_test.json', 'w') as f:
        #    json.dump(final_dict, f)

    print(final_dict)
    with open('results_part3_test.json', 'w') as f:
        json.dump(final_dict, f)


def part2():
    pass
    
if __name__ == "__main__":
    #preprocess_weather()
    data_stock = stock_preprocess_new()
    data_weather = preprocess_weather()
    data_comb = time_align(data_stock,data_weather)
    #print(data_comb.show())
    part3(data_comb)
    #print(data_comb)
    #print(data_comb.show())
