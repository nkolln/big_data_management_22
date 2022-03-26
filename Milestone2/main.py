from ipaddress import ip_address
from sqlite3 import InternalError
import pyspark.sql.functions as pf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Window,udf
from pyspark.rdd import reduce
import time as t
from pyspark.sql.functions import udf, array
from pyspark.sql.types import DoubleType
from ipaddress import ip_address, ip_network
import ipaddress

table_schema = StructType([StructField('sid', IntegerType(), True),
                     StructField('pid', IntegerType(), True)])

spark = SparkSession.builder.getOrCreate()
df_raw = spark.read.schema(table_schema).option("header",False).csv("MS2Test.txt")
time1 = t.time()
#df_raw = df_raw.withColumn("pid", convertUDF(pf.col("pid")))
#df_raw = df_raw.withColumn("pid", pf.abs(((pf.lit(3173119)*df_raw["pid"]) + pf.lit(442551)) % pf.lit(12077773)) % pf.lit(120011))
df_group = df_raw.groupBy(pf.col('sid'),pf.col('pid')).agg(pf.count(pf.col("sid")).alias('count'))#.drop("pid")
df_group2 = df_group.alias('df_group2')
df_group2 = df_group.select(pf.col('sid').alias('sid2'),pf.col('count').alias('count2'),pf.col('pid').alias('pid2'))
df_joined = df_group.crossJoin(df_group2)
df_joined = df_joined.filter((df_joined.sid < df_joined.sid2)&(df_joined.pid==df_joined.pid2))
df_joined = df_joined.withColumn('Comb',pf.col('count')*pf.col('count2'))
df_sum = df_joined.groupBy(pf.col('sid'),pf.col('sid2')).agg(pf.sum(pf.col('Comb')).alias('sum'))
df_sum = df_sum.filter(pf.col('sum')>3000)
print(t.time()-time1)
df_sum.collect()
print(t.time()-time1)
df_sum = df_sum.agg(pf.count(pf.col('sum')))
print(t.time()-time1)
df_sum.show()
print(t.time()-time1)
print(df_sum.count())
print(t.time()-time1)
df_sum.show()
#df_joined.show()
#df_joined = df_group.join(df_group2,(df_group.sid == df_group2.sid2)&(df_group.pid==df_group2.pid2),"inner")
#df_joined.show(50)

#df_offers = df_joined.withColumn("c", dot_udf(array('count', 'count2')))
#df_offers.show()

'''df_pivot = df_raw.groupBy(pf.col("pid")).pivot("sid").agg(pf.count(pf.col("pid"))).fillna(0)
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
dot_sums.show()'''

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