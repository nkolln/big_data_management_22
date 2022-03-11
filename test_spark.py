import time

from pyspark.sql import SparkSession, functions
import random
import pandas
import re
spark = SparkSession.builder.getOrCreate()
textFile = spark.read.text("Data/MS1.txt")
# print(textFile.filter(textFile.value.contains("USA")).filter(textFile.value.contains("2016")).count())
usa_filter = textFile.filter(textFile.value.contains("USA"))
year_filter = usa_filter.filter(usa_filter.value.contains("/2016") | usa_filter.value.contains("/2017") | usa_filter.value.contains("/2018") | usa_filter.value.contains("/2019"))
# print("year filter count: ", year_filter.count())
# year_filter.show(n=263, truncate=False)
ids = year_filter.select(functions.substring_index(year_filter.value, ".", 1).alias("value"))
# ids.show(n=267, truncate=False)
# print("unique ids: ", ids.distinct().count())
ids_unique = ids.dropDuplicates()
id_list = ids_unique.collect()
# print(id_list)
# spark.sparkContext.setCheckpointDir("Checkpoint")
# final_filter = year_filter

final_filter = year_filter
for i in range(8907):
    print("i: ",i)
    random_index = random.choice(range(len(id_list)))
    # removed = id_list[random_index]['value']
    id_list.remove(id_list[random_index])

print("creating final df...")
for i in range(len(id_list)):
    print("i: ", i)
    if i == 0:
        final_filter = year_filter.filter(functions.substring_index(year_filter.value, '.', 1) == id_list[i]['value'])
    else:
        final_filter = final_filter.union(year_filter.filter(functions.substring_index(year_filter.value, '.', 1) == id_list[i]['value']))

final_filter.write.option("header",True).text(r"D:\big_data_management_22\dateTest")
# final_filter.toPandas().to_csv("Data/test.csv")
# for i in range(8909):
#     # start = time.time()
#     random_index = random.choice(range(len(id_list)))
#     removed = id_list[random_index]['value']
#     id_list.remove(id_list[random_index])
#     # print("i: ", i)
#     # print("removed stock: ", removed)
#     # final_filter = final_filter.filter(functions.substring_index(final_filter.value, '.', 1) != removed)
#     if i == 0:
#         final_filter = year_filter.filter(functions.substring_index(year_filter.value, '.', 1) == removed)
#     else:
#         final_filter = final_filter.union(year_filter.filter(functions.substring_index(year_filter.value, '.', 1) == removed))
#     if i % 100 == 0 or i == 8908:
#         print("i: ", i)
    # print("time: ",time.time()-start)


# ids_unique = ids_unique.filter(ids_unique.value != random_id)

# ids_unique.show(n=200)


# for i in range(8909):
#     random_id = ids_unique.rdd.takeSample(False, 1, seed=None)
#     final_filter = year_filter.filter(year_filter.value != random_id)
#     ids_unique = ids_unique.filter(ids_unique.value != random_id)
#
# print("number of rows in final: ", final_filter.count())
# dates = year_filter.select(functions.regexp_extract(year_filter.value, "([0-9]{2}\\/[0-9]{2}\\/[0-9]{4})", 1))

# dates.write.option("header", True).csv(r"D:\big_data_management_22\dateTest")


