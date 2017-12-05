from pyspark import SparkContext, SQLContext

# Ceontext initialization
sc = SparkContext()
sqlContext = SQLContext(sc)

# Load the preprocessed parquet file (produced by compactify.py)
data_dir = 'hdfs:///user/cyu/'
df = sqlContext.read.parquet(data_dir + 'complete.parquet')

# For each product, count the number of reviews, then sort by count
product_count = df.groupby('asin') \
	.count() \
	.orderBy('count', ascending=False)

# Save the dataframe as a .parquet file
product_count.write.parquet(data_dir + 'product_sortby_review_count.parquet')

# Write the ID of top products in a text file
product_count.select('asin') \
	.limit(10000) \
	.write.text(data_dir + 'top_product.txt')



