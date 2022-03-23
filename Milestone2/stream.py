

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, functions
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

sc = SparkContext()
ssc = StreamingContext(sc, 2)

lines = ssc.socketTextStream("localhost", 9000)

words = lines.flatMap(lambda line: line.split(" "))

pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

wordCounts.pprint()

ssc.start()
ssc.awaitTermination()

