from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import BooleanType
from nltk.corpus import stopwords
from nltk.util import bigrams

DATA_PATH = 'hdfs:///user/mrizzo/filtered_df'
JSON_PATH = 'hdfs:///user/mrizzo/incent_json'

def filter_func(tokens, stops):
    lower = [t.lower() for t in tokens]
    nostops = [w for w in lower if w not in stops.value and len(w) > 2]
    bg = set(bigrams(nostops))

    return (('unbiased', 'review') in bg) or \
           (('honest', 'review') in bg) or \
           ('disclaimer' in nostops) or \
           (('exchange', 'review') in bg) or \
           (('exchange', 'unbiased') in bg) or \
           (('exchange', 'honest') in bg)


def main():
    sc = SparkContext()
    sc.addPyFile('nltk.zip')

    sqlContext = SQLContext(sc)
    sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

    df = sqlContext.read.parquet(DATA_PATH)

    stops = sc.broadcast(set(stopwords.words('english')))

    udf = UserDefinedFunction(lambda t: (filter_func(t, stops)), BooleanType())

    filtered = df.filter(udf(df.tokenized_text))

    filtered.write.mode('overwrite').json(JSON_PATH)


if __name__ == '__main__':
    main()
