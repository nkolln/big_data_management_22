import pyspark.sql.functions as pf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Window

table_schema = StructType([StructField('sid', IntegerType(), True),
                     StructField('pid', IntegerType(), True)])

spark = SparkSession.builder.getOrCreate()
df_raw = spark.read.schema(table_schema).option("header",False).csv("many_addresses.txt")
df_raw.show()
#df_raw = df_raw.withColumn("sid", df_raw["sid"].cast(IntegerType()))
df_raw = df_raw.withColumn('index', pf.monotonically_increasing_id().cast(IntegerType()))
#window = Window.orderBy("index").rowsBetween(0, 10)
df_raw.show()
df_raw.where((pf.col("index")>100) & (pf.col("index")<110)).show()
#df_raw.select("sid","pid",pf.row_number().over(window)).show()