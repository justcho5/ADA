import json
import numpy as np
from pyspark import SparkContext, SQLContext

NON_INCENT_PATH = 'hdfs:///user/mrizzo/non_incent_df'
INCENT_PATH = 'hdfs:///user/mrizzo/incent_df'


def get_plot_data(df, sqlContext, bins):
    if 'data' in sqlContext.tableNames():
        sqlContext.dropTempTable('data')

    sqlContext.registerDataFrameAsTable(df, 'data')
    queries = {
        'avg_compound_by_rating':
            'SELECT overall, AVG(compound_score) FROM data GROUP BY overall',
        'avg_length_by_overall':
            'SELECT overall, AVG(word_count) FROM data GROUP BY overall',
        'avg_length_by_price_tier':
            'SELECT price_tier, AVG(word_count) FROM data GROUP BY price_tier',
        'avg_compound_by_category':
            'SELECT main_category, AVG(compound_score) FROM data GROUP BY main_category',
        'avg_length_by_category':
            'SELECT main_category, AVG(word_count) FROM data GROUP BY main_category',
        'num_reviews_by_overall':
            'SELECT overall, COUNT(*) FROM data GROUP BY overall',
        'num_reviews_by_price_tier':
            'SELECT price_tier, COUNT(*) FROM data GROUP BY price_tier',
        'num_reviews_by_category':
            'SELECT main_category, COUNT(*) FROM data GROUP BY main_category'
    }

    ret = {
        name: [row.asDict() for row in sqlContext.sql(query).collect()]
        for (name, query) in queries.iteritems()
    }

    hist = [list(bins.value)]
    for rating in xrange(1, 6):
        hist.append(df.filter(df.overall == rating) \
                    .select('compound_score') \
                    .rdd \
                    .histogram(list(bins.value))[1])

    ret['sentiment_distribution_by_rating'] = hist

    return ret


def main():
    sc = SparkContext()

    sqlContext = SQLContext(sc)
    sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

    df_non_incent = sqlContext.read.parquet(NON_INCENT_PATH)
    df_incent = sqlContext.read.parquet(INCENT_PATH)

    non_incent_bootstap_samples = [df_non_incent.sample(True, 1.) for _ in xrange(10)]
    incent_bootstap_samples = [df_incent.sample(True, 1.) for _ in xrange(10)]

    bins = sc.broadcast(np.linspace(-1, 1, 11))
    incent_results = [get_plot_data(s, sqlContext, bins) for s in incent_bootstap_samples]
    non_incent_results = [get_plot_data(s, sqlContext, bins) for s in non_incent_bootstap_samples]

    with open('plots/incent_results.json', 'w') as f:
        json.dump(incent_results, f)

    with open('plots/non_incent_results.json', 'w') as f:
        json.dump(non_incent_results, f)


if __name__ == '__main__':
    main()
