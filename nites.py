from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read.option("header",True).text("Data/MS1.txt")
#print(df)
def elim(df):
    for i in range(len(df.columns)):
        if df[df.columns[i]].dtype != 'float64':
            print(df[df.columns[i]].dtype, i)

import pyspark.sql.functions as f
df_medium = spark.read.option("header",True).csv("Data/medium_test.csv")
#print(df)
def elim(df):
    for i in range(len(df.columns)):
        if df[df.columns[i]].dtype != 'float64':
            print(df[df.columns[i]].dtype, i)

df_medium.select(f.col('daily_timestamp1')).show()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

sqlContext = SQLContext(spark)

#from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, size, length
from pyspark.sql.functions import struct

import math
def cos_similarity(X, Y_agg):
    tot_numerator = 0
    for i in range(length(X)):
        tot_numerator += X[i] * Y_agg[i]
    tot_denominator = math.sqrt(sum(i**2 for i in X)) * math.sqrt(sum(j**2 for j in Y_agg))

    cos_sim = tot_numerator/tot_denominator
    return cos_sim

from pyspark.sql.functions import least, greatest
df = df.withColumn('min', least(col('Price'), col('Quantity')))
df.show()

df = df.withColumn('max', greatest(col('Price'), col('Quantity')))
df.show()

max_udf=udf(lambda x,y: greatest(x,y), IntegerType())
df2 = df.withColumn("result", max_udf(df.Quantity, df.Price))
df2.show()