from sqlite3 import InternalError
import pyspark.sql.functions as pf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.rdd import reduce
import time as t

table_schema = StructType([StructField('sid', IntegerType(), True),
                     StructField('pid', IntegerType(), True)])

spark = SparkSession.builder.getOrCreate()
df_raw = spark.read.schema(table_schema).option("header",False).csv("MS2Test.txt")
time1 = t.time()
#df_raw = df_raw.withColumn("hash", pf.hash(pf.col("pid")))
#df_raw = df_raw.withColumn("hash2", pf.abs(((pf.lit(-4387413)*df_raw["pid"]) + pf.lit(442551)) % pf.lit(432426133)) % pf.lit(120011))
df_pivot = df_raw.groupBy(pf.col("pid")).pivot("sid").agg(pf.count(pf.col("pid"))).fillna(0)
df_pivot = df_pivot.drop("pid")
products = list()
print(t.time()-time1)
time1 = t.time()
for c in df_pivot.columns:
    if "pid"!= c:
        for c2 in df_pivot.columns:
            if "pid"!= c2 and int(c) < int(c2):
                #print(f"{c} {c2}")
                #cols.append(df_pivot.select( pf.sum(pf.col(c) * pf.col(c))).alias(f"{c}"))
                products.append(pf.sum(pf.col(c) * pf.col(c2)).alias(f"{c}_{c2}"))
print(t.time()-time1)
dot_sums = df_pivot.select(*products)
print("dot_sums")
print(t.time()-time1)
dot_sums.show()

'''
print([pf.count(pf.when((pf.isnan(c)),c).alias(c)) for c in dot_sums.columns])
dot_sums.where(dot_sums>3000).show()
col_values = pf.explode(
    pf.array(*[pf.struct(pf.lit(c).alias("col_name"), pf.col(c).alias("val")) for c in dot_sums.columns])
).alias("cols_values")
print("col_values")
print(t.time()-time1)
df_final = dot_sums.select(col_values)
df_final.show()
""".select(*[pf.split(pf.col("cols_values.col_name"), "_").getItem(0).alias("n_1"),
            pf.split(pf.col("cols_values.col_name"), "_").getItem(1).alias("n_2"),
            pf.col("cols_values.val").alias("dot")])"""

df_final.show()
df_final.where(pf.col("dot")>3000).show()


print(t.time()-time1)
'''