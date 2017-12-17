from pyspark import SparkContext, SQLContext

JOINED_PATH = 'hdfs:///user/mrizzo/joined_df'
FILTERED_PATH = 'hdfs:///user/mrizzo/filtered_df'


def main():
    """
    Filter the joined dataframe by removing reviews with no price or no category
    """
    sc = SparkContext()

    sqlContext = SQLContext(sc)
    sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

    # Load
    df = sqlContext.read.parquet(JOINED_PATH)

    # Filter
    filtered = df.filter("price = 0 or main_category = ''")

    # Save
    filtered.write.mode('overwrite').parquet(FILTERED_PATH)


if __name__ == '__main__':
    main()
