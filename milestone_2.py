import time

from pyspark.sql import SparkSession
# import pyspark.sql

# from pyspark.sql.functions import split
from pyspark.sql.types import *
# from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
# from pyspark.ml.linalg import Vector, DenseVector
# from pyspark import SparkContext
# from pyspark.mllib.stat import Statistics
# from pyspark.sql.functions import col, countDistinct
# import pyspark.sql.functions as pf
# from pyspark.sql.functions import pandas_udf
# from pyspark.ml.feature import StringIndexer
# from pyspark.streaming import StreamingContext
# from pyspark import StorageLevel

import pandas
# import numpy as np
# import math
# import json

# import ipaddress


# spark = SparkSession \
#     .builder \
#     .appName("MS2") \
#     .getOrCreate()
#
# # read data
# df = spark.read.text("Data/MS2Test.txt")

# # convert data to a csv-like format
# df = df.withColumn("server", split(df.value, ",").getItem(0)).withColumn("ip", split(df.value, ",").getItem(1)).drop(
#     "value")
# # convert the data type of both columns to int
# df = df.withColumn("server", df["server"].cast(IntegerType())).withColumn("ip", df["ip"].cast(IntegerType()))

#create schema
table_schema = StructType([StructField('server', IntegerType(), True),
                           StructField('ip', IntegerType(), True)])

spark = SparkSession.builder.getOrCreate()
# enable pyarrow for faster conversion to pandas
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set(
    "spark.sql.execution.arrow.pyspark.fallback.enabled", "true"
)

# read data
df = spark.read.schema(table_schema).option("header", False).csv("Data/MS2Test.txt")

start = time.time()


# df = df.withColumn("ip", pf.abs(((pf.lit(-4387413) * df["ip"]) + pf.lit(442551)) % pf.lit(432426133)) % pf.lit(120011))
# convert data to pandas dataframe
df = pandas.DataFrame(df.toPandas())
# convert_pandas_end = time.time()
# df["ip"] = df["ip"].apply(lambda x: np.abs(((-4387413 * x) + 442551) % 432426133) % 120011)

# group_start = time.time()
# group by server and ip, count the ips
df = df.groupby(["server", "ip"])["ip"].count().to_frame()
# group_end = time.time()

# rename_start = time.time()
# rename the column
df.rename(columns={'ip': 'count'}, inplace=True)
# convert server from index to column
df.reset_index(inplace=True, level=["server"])
# create a copy of the df and change the column names
df2 = df.rename(columns={'count': 'count2', "server": "server2"})
# rename_end = time.time()

# join_start = time.time()
# inner join the original and the copy of the df on to the ip which is the index of both of them
joined_df = df.join(df2, how='inner')
# join_end = time.time()

# # df = df.sort("server", "ip")
# # df = df.alias("df")
# # df2 = df.alias("df2")
# df2 = df2.toDF("server2", "ip2", "count2")
# joined_df = df.join(df2, df["ip"] == df2["ip2"], "inner").alias("joined_df")
# joined_df = pandas.DataFrame(joined_df.toPandas())

# dot_start = time.time()
#  group the joined dataframe by server ids and calculate the dot product of count columns
joined_df = joined_df.groupby(["server", "server2"]).apply(
     lambda k: k['count'].dot(k['count2'])).to_frame()  # sum(k['count']*k['count2'])
# dot_end = time.time()

# result_start = time.time()
# get the rows where server id 1 is smaller than server id 2 (basically removing the duplicate rows and rows where ids are equal)
joined_df = joined_df[(joined_df.index.get_level_values(0) < joined_df.index.get_level_values(1))]
# get the number of rows where similarity calculated by the dot product is larger than 3000
res = joined_df[joined_df >= 3000].dropna().shape[0]
# result_end = time.time()

print("finished in: ", time.time() - start)
# print("dot finished in: ", dot_end - dot_start)
# print("join finished in: ", join_end - join_start)
# print("result finished in: ", result_end - result_start)
# print("convert finished in: ", convert_pandas_end - start)
# print("rename finished in: ", rename_end - rename_start)
# print("group finished in: ", group_end - group_start)

print(res)


# joined_df = joined_df.groupBy(joined_df["server"], joined_df["server2"]).applyInPandas(dot_product, "server1 int, server2 int, similarity int")

# joined_df.show(truncate=False)

# df.createOrReplaceTempView("df")
#
# new_df = spark.sql("SELECT * FROM df INNER JOIN df as df2 ON (first(df.ip) = first(df2.ip)) GROUP BY df.server, df2.server")
# new_df2 = spark.sql("SELECT df.server, df2.server FROM df, df2 WHERE df.server > df2.server SUM(df.count * df2.count)")
# new_df2.show(truncate=False)

# df.show()
# convert to pandas dataframe
# df = pandas.DataFrame(df.toPandas())
# # convert the data frame into a frequency table (takes 11 seconds)
# df = pandas.crosstab(df.loc[:, "server"], df.loc[:, "ip"], rownames=["server"], colnames=["ip"])
# print("conversion to wide format finished in: ", time.time()-start)
#
# # take the dot product of the frequency table with its transpose
# answer = pandas.DataFrame(df.dot(df.transpose()))
# # take the lower triangle from the product result
# answer = np.tril(answer, -1)
# # count the values larger than 3000 inside the answer
# print("number of similar servers: ", (answer >= 3000).sum())
# print("finished in: ", time.time()-start)


# print(df)
# df = df.filter(df.server.between(0, 1))
# df. show(n=60, truncate=False)

# df = df.crosstab("server", "ip")

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
