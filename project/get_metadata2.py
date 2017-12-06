import re
from pyspark.sql.types import BooleanType
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import UserDefinedFunction

# context initialization
sc = SparkContext()
sqlContext = SQLContext(sc)

# list of asin to sample, currently it is the list of products with most number of reviews
lookup = {}
with open('/buffer/top_product.txt/part-r-00000-c931be60-9b2b-4382-87a0-68a5283992df') as f:
	lookup = sc.broadcast(set(map(lambda s: s.strip(), f)))

# read the preprocessed metadata, need to run 'preprocess_metadata.py' to get 'processed_metadata.parquet'
data_dir = 'hdfs:///user/cyu/'
metadata = sqlContext.read.parquet(data_dir + 'processed_metadata.parquet')

# return True if this product ID is in the lookup list
def filter_func(asin):
	if asin:
		return asin in lookup.value
	else:
		return False

# add filter_func as a user defined function
udf = UserDefinedFunction(filter_func, BooleanType())

# add a new column indicating filter or not, and apply filter() on it
metadata = metadata.withColumn('keep', udf(metadata.asin))
filtered = metadata.filter(metadata.keep)

# save as parquet
filtered.write.parquet(data_dir + 'top_product_meta.parquet')
