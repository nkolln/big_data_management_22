import time

from pyspark.sql import SparkSession
import pyspark.sql
from pyspark.sql.functions import split
from pyspark.sql.types import *
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from pyspark.ml.linalg import Vector, DenseVector
from pyspark import SparkContext
from pyspark.mllib.stat import Statistics
from pyspark.sql.functions import col, countDistinct
import pyspark.sql.functions as pf
from pyspark.ml.feature import StringIndexer
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel

import pandas
import numpy as np
import math
import json

import ipaddress

spark = SparkSession \
    .builder \
    .appName("MS2") \
    .getOrCreate()

# read data
df = spark.read.text("Data/MS2Test.txt")
start = time.time()
# convert data to a csv-like format
df = df.withColumn("server", split(df.value, ",").getItem(0)).withColumn("ip", split(df.value, ",").getItem(1)).drop(
    "value")
# convert the data type of both columns to int
df = df.withColumn("server", df["server"].cast(IntegerType())).withColumn("ip", df["ip"].cast(IntegerType()))
# apply hash function on the ip column
df = df.withColumn("ip", pf.abs(((pf.lit(-4387413)*df["ip"]) + pf.lit(442551)) % pf.lit(432426133)) % pf.lit(120011))
# convert to pandas dataframe
df = pandas.DataFrame(df.toPandas())
# convert the data frame into a frequency table (takes 11 seconds)
df = pandas.crosstab(df.loc[:, "server"], df.loc[:, "ip"], rownames=["server"], colnames=["ip"])
print("conversion to wide format finished in: ", time.time()-start)

# take the dot product of the frequency table with its transpose
answer = pandas.DataFrame(df.dot(df.transpose()))
# take the lower triangle from the product result
answer = np.tril(answer, -1)
# count the values larger than 3000 inside the answer
print("number of similar servers: ", (answer >= 3000).sum())
print("finished in: ", time.time()-start)


# print(df)
# df = df.filter(df.server.between(0, 1))
# df. show(n=60, truncate=False)

# df = df.crosstab("server", "ip")
# df = df.groupBy("server", "ip").count()
# # # df = df.withColumn("id", pf.monotonically_increasing_id())
#
# df = df.sort("server")
# df.show(n=100)
# print("starting cross..")
# df = df.groupBy("server").pivot("ip").agg(pf.first("count"))
# print("conversion to wide format finished in: ", time.time()-start)
# print("df after cross:")
# df.show(truncate=False)
# df = df.withColumn("server_ip", df["server_ip"].cast(IntegerType()))
# # df.printSchema()
# df = df.sort("server_ip")
# # df.show(truncate=False)
# df = df.drop("server_ip")
#
# pandas_df = df.toPandas()
#

