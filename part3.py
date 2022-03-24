from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import math
import json
import time

spark = SparkSession.builder.getOrCreate()
data = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("Data/medium_test.csv")
data = data.drop("_c0", "daily_timestamp1", "daily_timestamp963")
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
print(stock_names)
print(county_names)
final_dict = {}


result_index = 0
start = time.time()
for stock in stock_names:
    print(stock)
    # stock = float(stock)
    # print(stock)
    # stock = str(stock)
    # print(stock)
    print("result index:", result_index)
    county_index = 0
    for i, county1 in enumerate(county_names):
        print("county_index:", county_index)
        rest_of_the_counties = county_names[i+1:]

        single_iter_start = time.time()

        for count2 in rest_of_the_counties:
            print("county2: ",count2)
            # min = data.withColumn("min", least(col(county1), col(count2)))
            # min_sq = min.select(pow("min", 2).alias("min_sq")).agg({'min_sq': 'sum'}).collect()[0][0]
            # min_sqrt = math.sqrt(min_sq)
            # print("min_sqrt: ", min_sqrt)


            max = data.withColumn("max", greatest(col(county1), col(count2)))
            max_sq = max.select((pow("max", 2)).alias("max_sq")).agg({'max_sq': 'sum'}).collect()[0][0]
            max_sqrt = math.sqrt(max_sq)
            # print("max_sqrt: ", max_sqrt)

            avg = data.withColumn("avg", (col(county1) + col(count2)) / lit(2))
            avg_sq = avg.select(pow("avg", 2).alias("avg_sq")).agg({'avg_sq': 'sum'}).collect()[0][0]
            avg_sqrt = math.sqrt(avg_sq)
            # print("avg_sqrt: ", avg_sqrt)

            stocks = data.select(col(stock).alias("stocks"))
            stocks_sq = stocks.select(pow("stocks", 2).alias("stocks_sq")).agg({'stocks_sq': 'sum'}).collect()[0][0]
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

        single_iter_end = time.time() - single_iter_start
        print("single_iter_end: ", single_iter_end)

        county_index += 1

    result_index += 1
    with open('results_part3_test.json', 'w') as f:
        json.dump(final_dict, f)

end = time.time() - start
print("end: ", end)
with open('results_part3_test.json', 'w') as f:
    json.dump(final_dict, f)

print("part 3 done.")
