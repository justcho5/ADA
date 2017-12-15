import json
import numpy as np
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import BooleanType

DATA_PATH = 'hdfs:///user/mrizzo/filtered_df'

def main():
    sc = SparkContext()

    sqlContext = SQLContext(sc)
    sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

    df = sqlContext.read.parquet(DATA_PATH)

    bins = sc.broadcast(np.linspace(-1,1,11))
    hist = [list(bins.value)]
    for rating in range(1,6):
        hist.append(df.filter(df.overall==rating) \
                      .select('compound_score') \
                      .rdd \
                      .histogram(list(bins.value))[1])
    with open('plots/sent_rating.txt','w') as f:
        json.dump(hist, f)

    sqlContext.registerDataFrameAsTable(df, "ALL_REVIEW")
    rows = sqlContext.sql("SELECT overall, AVG(compound_score) FROM ALL_REVIEW GROUP BY overall").collect()
    jsons = [row.asDict() for row in rows]
    with open('plots/rating_compound.txt','w') as f:
        json.dump(jsons, f)

    rows = sqlContext.sql("SELECT overall, AVG(word_count) FROM ALL_REVIEW GROUP BY overall").collect()
    jsons = [row.asDict() for row in rows]
    with open('plots/rating_length.txt','w') as f:
        json.dump(jsons, f)

    rows = sqlContext.sql("SELECT price_tier, AVG(word_count) FROM ALL_REVIEW GROUP BY price_tier").collect()
    jsons = [row.asDict() for row in rows]
    with open('plots/price_length.txt','w') as f:
        json.dump(jsons, f)

    rows = sqlContext.sql("SELECT main_category, AVG(compound_score) FROM ALL_REVIEW GROUP BY main_category").collect()
    jsons = [row.asDict() for row in rows]
    with open('plots/compound_category.txt','w') as f:
        json.dump(jsons, f)

    rows = sqlContext.sql("SELECT main_category, AVG(word_count) FROM ALL_REVIEW GROUP BY main_category").collect()
    jsons = [row.asDict() for row in rows]
    with open('plots/length_category.txt','w') as f:
        json.dump(jsons, f)

if __name__ == '__main__':
    main()
