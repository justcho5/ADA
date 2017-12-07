import json
import bisect
import ast
from pyspark import SparkContext, SQLContext

categories_dict_path = '/home/mrizzo/categories_dict.json'
META_PATH = 'hdfs:///datasets/productGraph/metadata.json'
OUTPUT_PATH = 'hdfs:///user/mrizzo/meta_df'

price_bins = [
    0,
    10,
    20,
    30,
    40,
    50,
    60,
    70,
    80,
    90,
    100,
    200,
    500,
    1000
]


def add_price_tier(row, bins):
    # We don't want any reviews for which the price is either NaN or 0
    # or which don't belong to any category
    if ('price' not in row) or (row['price'] == 0):
        tier = 0
    else:
        tier = bisect.bisect_left(bins.value, row['price'])

    # price tier
    row['price_tier'] = tier
    return row


def add_main_category(row, categories_dict):
    # main category
    def get_main_category(categories):

        # flaten a list of lists
        flat = set(sum(categories, []))

        # map category names, trim None
        main_categories = list(filter(None, map(categories_dict.value.get, flat)))

        # uniquify, not needed if return only one category
        # main_categories = list(set(main_categories))

        # return the first category, if exists
        if len(main_categories) > 0:
            return main_categories[0]
        else:
            return ''

    if 'categories' not in row:
        row['main_category'] = ''
    else:
        row['main_category'] = get_main_category(row['categories'])

    # remove items for which there is no category
    # if row['main_category']:
    # return None

    return row


def main():
    # context initialization
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

    bins = sc.broadcast(price_bins)

    # Load category dictionary from file
    with open(categories_dict_path) as f:
        categories_dict = sc.broadcast(json.load(f))

    # read the input file line by line, then load strings as jsons
    metadata = sc.textFile(META_PATH) \
        .map(ast.literal_eval) \
        .map(lambda r: add_price_tier(r, bins)) \
        .map(lambda r: add_main_category(r, categories_dict))
    # .filter(lambda x: x)

    # convert to dataframe
    metadata_df = sqlContext.createDataFrame(metadata)

    print('{} metadata rows'.format(metadata_df.count()))

    # save as parquet
    metadata_df.write.mode('overwrite').parquet(OUTPUT_PATH)


if __name__ == '__main__':
    main()
