import json
import re
from pyspark.sql import *
from pyspark import SparkContext, SQLContext

# context initialization
sc = SparkContext()
sqlContext = SQLContext(sc)

# read the input file line by line
#sampled_text.saveAsTextFile('hdfs:///user/cyu/sample0.01.json')
text_file = sc.textFile('hdfs:///user/cyu/sample_cl_0.01.json/part-00000')
#text_file = sc.textFile('hdfs:///datasets/productGraph/complete.json')


reviews = text_file.map(lambda line: json.loads(line)) \
    .reduce(lambda a, b: a + b)

reviews.write.parquet('hdfs:///user/cyu/part-00000.parquet')
