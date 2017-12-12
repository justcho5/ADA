from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import BooleanType

DATA_PATH = 'hdfs:///user/mrizzo/bigrams_df'
INCENT_PATH = 'hdfs:///user/mrizzo/incent_df'

def filter_func(bigrams):
    bg = set([tuple(b) for b in bigrams])

    return (('complimentary', 'copy') in bg) or \
           (('discount', 'exchange') in bg) or \
           (('exchange', 'product') in bg) or \
           (('exchange', 'review') in bg) or \
           (('exchange', 'unbiased') in bg) or \
           (('exchange', 'free') in bg) or \
           (('exchange', 'honest') in bg) or \
           (('exchange', 'true') in bg) or \
           (('exchange', 'truth') in bg) or \
           (('fair', 'review') in bg) or \
           (('free', 'discount') in bg) or \
           (('free', 'exchange') in bg) or \
           (('free', 'sample') in bg) or \
           (('free', 'unbiased') in bg) or \
           (('honest', 'feedback') in bg) or \
           (('honest', 'unbiased') in bg) or \
           (('opinion', 'state') in bg) or \
           (('opinion', 'own') in bg) or \
           (('provide', 'exchange') in bg) or \
           (('provide', 'sample') in bg) or \
           (('provided', 'sample') in bg) or \
           (('provided', 'exchange') in bg) or \
           (('receive', 'free') in bg) or \
           (('receive', 'free') in bg) or \
           (('received', 'free') in bg) or \
           (('received', 'sample') in bg) or \
           (('return', 'unbiased') in bg) or \
           (('review', 'sample') in bg) or \
           (('sample', 'product') in bg) or \
           (('sample', 'unbiased') in bg) or \
           (('sample', 'free') in bg) or \
           (('send', 'sample') in bg) or \
           (('unbiased', 'review') in bg) or \
           (('unbiased', 'opinion') in bg) or \
           (('unbiased', 'view') in bg)


def main():
    sc = SparkContext()

    sqlContext = SQLContext(sc)
    sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

    df = sqlContext.read.parquet(DATA_PATH)

    udf = UserDefinedFunction(filter_func, BooleanType())

    filtered = df.filter(udf(df.bg))

    filtered.write.mode('overwrite').parquet(INCENT_PATH)


if __name__ == '__main__':
    main()
