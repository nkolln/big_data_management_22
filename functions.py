from pandas import DataFrame
from pyspark.mllib.stat import Statistics
from pyspark.sql.functions import col, countDistinct
import pyspark.sql.functions as pf
from pyspark.ml.feature import StringIndexer


class Stats:
    def count_unique_mult(data:DataFrame)->None:
        out = data.agg(*(countDistinct(col(c)).alias(c) for c in data.columns))
        out.show()

    def count_unique(data:DataFrame,column:str)->None:
        data.agg(countDistinct(col(column)).alias("count")).show()

    def summary_statistics_rdd(data)->None:
        summary = Statistics.colStats(data)
        print(summary.mean())  # a dense vector containing the mean value for each column
        print(summary.variance())  # column-wise variance
        print(summary.numNonzeros())  # number 

class Transform:
    def categorical_conversion(data:DataFrame,col:str)->DataFrame:
        data = data.sort(pf.asc(col))
        #data.show()
        indexer = StringIndexer(inputCol=col, outputCol="category"+str(col))
        data = indexer.fit(data).transform(data)
        return(data)
