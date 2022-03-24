from pyspark.sql import SparkSession

def first_out():
    spark = SparkSession.builder.getOrCreate()
    rdd1 = spark.read.option("header",True).csv("/data.csv")
    print(rdd1.take(1))

    rdd2 = spark.read.text("file:/opt/data/MS1.txt")
    print(rdd2.take(1))

if __name__ == "__main__":
    first_out()