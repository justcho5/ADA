from pyspark import SparkContext, SQLContext

REVIEW_PATH = 'hdfs:///user/mrizzo/review_df'
META_PATH = 'hdfs:///user/mrizzo/meta_df'
JOINED_PATH = 'hdfs:///user/mrizzo/joined_df'


def main():
    """
    Join the reviews and metadata dataframe by using the asin (product ID) column
    """
    sc = SparkContext()

    sqlContext = SQLContext(sc)
    sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

    # Load
    reviews = sqlContext.read.parquet(REVIEW_PATH)
    meta = sqlContext.read.parquet(META_PATH)

    # Join
    joined = reviews.join(meta, on='asin')

    # Save
    joined.write.mode('overwrite').parquet(JOINED_PATH)


if __name__ == '__main__':
    main()
