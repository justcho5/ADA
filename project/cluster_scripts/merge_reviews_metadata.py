from pyspark import SparkContext, SQLContext

REVIEW_PATH = 'hdfs:///user/mrizzo/review_df'
META_PATH = 'hdfs:///user/mrizzo/meta_df'
JOINED_PATH = 'hdfs:///user/mrizzo/joined_df'

sc = SparkContext()

sqlContext = SQLContext(sc)
sqlContext.setConf('spark.sql.parquet.compression.codec','snappy')

reviews = sqlContext.read.parquet(REVIEW_PATH)
meta = sqlContext.read.parquet(META_PATH)

joined = reviews.join(meta, on='asin')

joined.write.mode('overwrite').parquet(JOINED_PATH)

print('{} rows'.format(joined.count()))
