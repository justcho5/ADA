import json
import numpy as np
from pyspark import SparkContext, SQLContext

NON_INCENT_PATH = 'hdfs:///user/mrizzo/non_incent_df'
INCENT_PATH = 'hdfs:///user/mrizzo/incent_df'
ALL_PATH = 'hdfs:///user/mrizzo/joined_df'
ELECTRONICS_PATH = 'hdfs:///user/cyu/electronics_df'
ELECTRONICS_INCENT_PATH = 'hdfs:///user/cyu/electronics_incent_df'
ELECTRONICS_NON_INCENT_PATH = 'hdfs:///user/cyu/electronics_non_incent_df'


def get_plot_data(df, sqlContext, bins):
    if 'data' in sqlContext.tableNames():
        sqlContext.dropTempTable('data')

    sqlContext.registerDataFrameAsTable(df, 'data')
    queries = {
        'avg_compound_by_overall':
            'SELECT overall, AVG(compound_score) FROM data GROUP BY overall',
        'avg_length_by_overall':
            'SELECT overall, AVG(word_count) FROM data GROUP BY overall',
        'avg_length_by_price_tier':
            'SELECT price_tier, AVG(word_count) FROM data GROUP BY price_tier',
        'avg_overall_by_price_tier':
            'SELECT price_tier, AVG(overall) FROM data GROUP BY price_tier',
        'avg_compound_by_category':
            'SELECT main_category, AVG(compound_score) FROM data GROUP BY main_category',
        'avg_length_by_category':
            'SELECT main_category, AVG(word_count) FROM data GROUP BY main_category',
        'num_reviews_by_overall':
            'SELECT overall, COUNT(*) FROM data GROUP BY overall',
        'num_reviews_by_price_tier':
            'SELECT price_tier, COUNT(*) FROM data GROUP BY price_tier',
        'num_reviews_by_category':
            'SELECT main_category, COUNT(*) FROM data GROUP BY main_category',
        'avg_overall':
            'SELECT AVG(overall) FROM data',
        'avg_compound':
            'SELECT AVG(compound_score) FROM data',
        'avg_length':
            'SELECT AVG(word_count) FROM data'
    }

    # Run all queries and store the results in a dict
    # The output of each query is very small so using collect() is fine
    ret = {
        name: [row.asDict() for row in sqlContext.sql(query).collect()]
        for (name, query) in queries.iteritems()
    }

    # Make a histogram of the sentiment scores
    # Add the histogram to the dict
    ret['compound_histogram'] = \
        df.select('compound_score').rdd.histogram(bins.value)[1]

    return ret


def main():
    """
    Compute statistics on both incentivized reviews and non-incentivized reviews
    using 10x bootstrap resampling and store the results in two JSON files
    that will be used to draw plots off the cluster.
    """
    num_bootstrap_samples = 10
    sc = SparkContext()

    sqlContext = SQLContext(sc)
    sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

    # Non incentivized reviews
    df_non_incent = sqlContext.read.parquet(NON_INCENT_PATH)
    # Incentivized reviews
    df_incent = sqlContext.read.parquet(INCENT_PATH)
    # Whole dataset
    df_whole = sqlContext.read.parquet(WHOLE_DATASET_PATH)
    # All reviews in the electronics category
    df_elec = sqlContext.read.parquet(ELECTRONICS_PATH)
    # Non incentivized reviews in the electronics category
    df_elec_non_incent = sqlContext.read.parquet(ELECTRONICS_NON_INCENT_PATH)
    # Incentivized reviews in the electronics category
    df_elec_incent = sqlContext.read.parquet(ELECTRONICS_INCENT_PATH)

    df_incent_electronics = df_incent.filter("main_category = 'Electronics'")
    df_non_incent_electronics = df_non_incent.filter("main_category = 'Electronics'")

    # Draw bootstrap samples from each dataframe
    non_incent_bootstap_samples = [df_non_incent.sample(True, 1.)
                                   for _ in range(num_bootstrap_samples)]
    incent_bootstap_samples = [df_incent.sample(True, 1.)
                               for _ in range(num_bootstrap_samples)]
    whole_dataset_bootstrap_samples = [df_whole.sample(True, 1.)
                                       for _ in range(num_bootstrap_samples)]
    elec_non_incent_bootstrap_samples = [df_non_incent_electronics.sample(True, 1.)
                                         for _ in range(num_bootstrap_samples)]
    elec_incent_bootstrap_samples = [df_incent_electronics.sample(True, 1.)
                                     for _ in range(num_bootstrap_samples)]

    # Bins for the sentiment score histogram
    bins = sc.broadcast(list(np.linspace(-1, 1, 11)))

    # Non-incentivized reviews (do this first because this dataframe is much
    # larger than just non incentivized so if Spark does not run out of memory
    # here we should be fine)
    non_incent_results = [get_plot_data(s, sqlContext, bins)
                          for s in non_incent_bootstap_samples]
    # Save the results
    with open('plots/non_incent_results.json', 'w') as f:
        json.dump(non_incent_results, f)

    # Whole dataset, similar size as only non incentivized
    whole_dataset_results = [get_plot_data(s, sqlContext, bins)
                             for s in whole_dataset_bootstrap_samples]
    with open('plots/whole_dataset_results.json', 'w') as f:
        json.dump(whole_dataset_results, f)

    # Incentivized only
    incent_results = [get_plot_data(s, sqlContext, bins)
                      for s in incent_bootstap_samples]
    with open('plots/incent_results.json', 'w') as f:
        json.dump(incent_results, f)

    # Electronics only
    elec_incent_results = [get_plot_data(s, sqlContext, bins)
                      for s in elec_incent_bootstrap_samples]
    with open('plots/elec_incent_results.json', 'w') as f:
        json.dump(elec_incent_results, f)

    elec_non_incent_results = [get_plot_data(s, sqlContext, bins)
                           for s in elec_non_incent_bootstrap_samples]
    with open('plots/elec_non_incent_results.json', 'w') as f:
        json.dump(elec_non_incent_results, f)


if __name__ == '__main__':
    main()
