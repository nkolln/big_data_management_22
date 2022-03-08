
dct_encoder = {"Light":1,"Moderate":2,"Severe":3}
def encode(in_str):
    return(dct_encoder[in_str])


def preprocess_weather()->None:
    spark = SparkSession.builder.getOrCreate()
    df_raw = spark.read.option("header",True).csv("Data/head.csv")

    df_processed = df_raw.withColumn("daily_timestamp", pf.date_trunc("day", df_raw['StartTime(UTC)']))

    #df_processed = df_processed.groupBy("daily_timestamp").agg(pf.avg(pf.col))

    pivot_df = df_processed.groupBy("daily_timestamp").pivot("County").agg(pf.collect_list(pf.col("Type")),pf.collect_list(pf.col("Severity")), pf.avg(pf.col("Precipitation(in)")))

    #pivot_df = df_raw.groupBy("StartTime(UTC)").pivot("County").agg(pf.first(pf.col("Type","Severity")))
    pivot_df.toPandas().to_csv('Data/weather.csv')



    from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as pf
import functions as f
from pyspark.sql.functions import col, countDistinct
from pyspark.ml.feature import StringIndexer
import pandas as pd

df = pd.read_csv('Data/head.csv')


"""
spark = SparkSession.builder.getOrCreate()


df_encode = spark.read.option("header",True).csv("Data/encoder.csv").show()
df_raw = spark.read.option("header",True).csv("Data/head.csv").show()
indexer = StringIndexer(inputCol="Severity", outputCol="categoryIndex")
model = indexer.fit(df_encode)
indexed = model.transform(df_raw)
indexed.show()
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data2 = [("0","","Smith","36636","M",3000),
    ("1","Rose","Brown","40288","M",4000),
    ("2","","Smith","42114","M",4000),
    ("3","Anne","Jones","39192","F",4000),
    ("4","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("id",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
indexer = StringIndexer(inputCol="lastname", outputCol="categoryIndex")
model = indexer.fit(df)
indexed = model.transform(df)
indexed.show()



spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(
    [(0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")],
    ["id", "category"])
print(df.toDF())
indexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
model = indexer.fit(df)
indexed = model.transform(df)
indexed.show()
"""