from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
textFile = spark.read.text("Data/MS1.txt")
print(textFile.filter(textFile.value.contains("NASDAQ")).count())
