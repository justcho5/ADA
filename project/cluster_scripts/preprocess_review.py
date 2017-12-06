import json
import datetime
import numpy as np
from pyspark import SparkContext, SQLContext

def process(line):
	row = json.loads(line)

	# We don't want any reviews for which the price is either NaN or 0
	# or which don't belong to any category
	if ('reviewText' not in row) or (len(row['reviewText'])==0):
		return None

	# Parse the review time as a DateTime and add review month and year columns
	row.update({'reviewTime':
				datetime.datetime.fromtimestamp(row['unixReviewTime'])})
	row.update({'reviewMonth': row['reviewTime'].month})
	row.update({'reviewYear': row['reviewTime'].year})
	
	# ToDo: helpful, word count, sentiment score
	
	return row

# context initialization
sc = SparkContext()
sqlContext = SQLContext(sc)

#review_path = 'hdfs:///datasets/productGraph/complete.json'
data_dir = 'C:/Users/AnnIe/Desktop/Courses/17F-AppliedDataAnalysis/ADA/project/data/'
review_path = data_dir + 'part-00000'
output_path = data_dir + 'processed_review.parquet'

# read the input file line by line, then load strings as jsons
# sample for local debug
review = sc.textFile(review_path) \
	.sample(withReplacement=False, fraction=0.001, seed=1) \
	.map(json.loads)

# convert to dataframe
review_df = sqlContext.createDataFrame(review)

# save as parquet
review_df.write.parquet(output_path)

