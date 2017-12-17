import json
import numpy as np
from pyspark import SparkContext, SQLContext

NON_INCENT_PATH = 'hdfs:///user/mrizzo/non_incent_df'
INCENT_PATH = 'hdfs:///user/mrizzo/incent_df'


def get_plot_data(df, sqlContext, bins):
    """
    Compute statistics on a Spark dataframe containing product reviews
    :param df: The Spark dataframe containing the data to analyze
    :param sqlContext: The Spark SQL context
    :param bins: Bins for the sentiment score histogram, as a Spark broadcast variable
    :return: A dict containing the results of the computations
    """
    # Register the dataframe as a SQL table and unregister any previous tables
    # with the same name
    if 'data' in sqlContext.tableNames():
        sqlContext.dropTempTable('data')

    # Register our dataframe as a SQL table
    sqlContext.registerDataFrameAsTable(df, 'data')
    # Queries to run
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

    # Run all queries and store the results in a dict
    ret = {
        # The output of each query is very small so using collect() is fine
        name: [row.asDict() for row in sqlContext.sql(query).collect()]
        for (name, query) in queries.iteritems()
    }

    # Make a histogram of the sentiment scores for each review score
    hist = [list(bins.value)]
    for rating in xrange(1, 6):
        hist.append(df.filter(df.overall == rating) \
                    .select('compound_score') \
                    .rdd \
                    .histogram(list(bins.value))[1])

    # Add the histogram to the dict
    ret['sentiment_distribution_by_rating'] = hist

    return ret


def main():
    """
    Compute statistics on both incentivized reviews and non-incentivized reviews
    using 10x bootstrap resampling and store the results in two JSON files
    that will be used to draw plots off the cluster.
    """
    sc = SparkContext()

    sqlContext = SQLContext(sc)
    sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

    # Non incentivized reviews
    df_non_incent = sqlContext.read.parquet(NON_INCENT_PATH)
    # Incentivized reviews
    df_incent = sqlContext.read.parquet(INCENT_PATH)

    # Draw 10 bootstrap samples from each dataframe
    non_incent_bootstap_samples = [df_non_incent.sample(True, 1.) for _ in xrange(10)]
    incent_bootstap_samples = [df_incent.sample(True, 1.) for _ in xrange(10)]

    # Bins for the sentiment score histogram
    bins = sc.broadcast(np.linspace(-1, 1, 11))

    # Non-incentivized reviews (do this first because this dataframe is much
    # larger than the other so if Spark does not run out of memory here we
    # should be fine)
    non_incent_results = [get_plot_data(s, sqlContext, bins)
                          for s in non_incent_bootstap_samples]
    # Save the results
    with open('plots/non_incent_results.json', 'w') as f:
        json.dump(non_incent_results, f)

    incent_results = [get_plot_data(s, sqlContext, bins)
                      for s in incent_bootstap_samples]
    with open('plots/incent_results.json', 'w') as f:
        json.dump(incent_results, f)


if __name__ == '__main__':
    main()
