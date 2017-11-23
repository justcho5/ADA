import json
import re
from pyspark.sql import *
from pyspark import SparkContext, SQLContext

# context initialization
sc = SparkContext()
sqlContext = SQLContext(sc)

# read the input file line by line
sampled_text.saveAsTextFile('hdfs:///user/cyu/sample0.01.json')
text_file = sc.textFile('hdfs:///datasets/productGraph/complete.json')

# convert a text line to words vector
def get_line_words(line):
    return word_regex.findall(line.lower())


counts_rdd = text_file.flatMap(get_line_words) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda wc: -wc[1])

# convert to dataframe
counts = sqlContext.createDataFrame(counts_rdd.map(lambda wc: Row(word=wc[0], count=wc[1])))

# view the content of the dataframe
counts.show()

# get 3 row for the dataframe
top3 = counts.take(3)
print("Top 3 words:")
for w in top3:
    print(w)

# save to json
counts.write.json("hdfs:///user/cyu/frankenstein_words_count.txt")
#counts.write.json("hdfs:///usr/cyu/words_count.txt")
#counts.write.json("C:/Users/AnnIe/Desktop/Courses/17F-AppliedDataAnalysis/wc.txt")
#counts.write.json("../wc.txt")
