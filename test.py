from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as pf
import functions as f
from pyspark.sql.functions import col, countDistinct
from pyspark.ml.feature import StringIndexer



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