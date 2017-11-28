import re
from pyspark import SparkContext


### This script uses Spark to extract metadata for products whose ID is in a given list stored as a text file with one
### ID per line. Since the metadata file is a text file with one JSON object per line we can load it as a RDD of lines
### and filter the lines so that only those whose ID is in the provided list remains.

# context initialization
sc = SparkContext()

lookup = None

with open('/buffer/asin_lookup.csv') as f:
  lookup = sc.broadcast([line.rstrip() for line in f])

# read the input file line by line
metadata = sc.textFile('hdfs:///datasets/productGraph/metadata.json')

asin_re = re.compile(r"{'asin': '([a-zA-Z0-9]+)'")

# return True if this line's product ID is in the lookup list
def filter_func(line):
    match = asin_re.match(line)
    return (match.group(1) in lookup.value) if match else False

lines = metadata.filter(filter_func)

# save to json with gzip compression
lines.saveAsTextFile('hdfs:///users/mrizzo/meta.gz', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')