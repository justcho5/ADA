import json
import re
from pyspark.sql import *
from pyspark import SparkContext, SQLContext

# context initialization
sc = SparkContext()
sqlContext = SQLContext(sc)

# read the input file line by line
#sampled_text.saveAsTextFile('hdfs:///user/cyu/sample0.01.json')
# text_file = sc.textFile('hdfs:///user/hcho/part-00000')
text_file = sc.textFile('hdfs:///datasets/productGraph/complete.json')

reviews = text_file.map(lambda line: json.loads(line))
df = sqlContext.createDataFrame(reviews)
df.write.parquet('hdfs:///user/hcho/complete.parquet')
# df.write.parquet('hdfs:///user/hcho/part-00000.parquet')