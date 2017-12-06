import json
import bisect
from pyspark import SparkContext, SQLContext

# context initialization
sc = SparkContext()
sqlContext = SQLContext(sc)

categories_dict_path = '/home/cyu/categories_dict.json'
metadata_path = 'hdfs:///datasets/productGraph/metadata.json'
output_path = 'hdfs:///user/cyu/processed_metadata.parquet'

# Load category dictionary from file
categories_dict = None
with open(categories_dict_path) as f:
	categories_dict = sc.broadcast(json.load(f))

price_bins = sc.broadcast([
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
])

def process(line):
	row = eval(line)

	# We don't want any reviews for which the price is either NaN or 0
	# or which don't belong to any category
	if 'categories' not in row:
		return None
	if ('price' not in row) or (row['price']==0):
		return None

	# price tier
	row.update({'price_tier': bisect.bisect_left(price_bins.value, row['price'])})
	
	# main category
	def get_main_category(categories):
	
		# flaten a list of lists
		flat = set(sum(categories, []))
	
		# map category names, trim None
		main_categories = filter(None, map(categories_dict.value.get, flat))

		# uniquify, not neede if return only one category
		#main_categories = list(set(main_categories))
		
		# return the first category, if exists
		if len(main_categories) > 0:
			return main_categories[0]
		else:
			return ''

	row['main_category'] = get_main_category(row['categories'])
	
	# remove items for which there is no category
	if row['main_category']:
		return None
	
	return row

# read the input file line by line, then load strings as jsons
metadata = sc.textFile(metadata_path) \
	.map(process) \
	.filter(lambda x: x)

# convert to dataframe
metadata_df = sqlContext.createDataFrame(metadata)

# save as parquet
metadata_df.write.parquet(output_path)


