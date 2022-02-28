from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
textFile = spark.read.text("Data/MS1.txt")
filt = "S&P"
text_filter = textFile.filter(textFile.value.contains(filt)|textFile.value.contains(filt.upper())|textFile.value.contains(filt.lower()))
print(text_filter.count())
#text_filter = text_filter.filter(text_filter.value.contains('car'))
#print(text_filter.count())
text_filter.toPandas().to_csv('Data/'+filt+'.csv')