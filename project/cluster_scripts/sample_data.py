from pyspark import SparkContext

# Script used to extract a random sample of the data from the full dataset using Spark.
# Since the dataset is a text file with one JSON object by line we can just use
# sc.textFile to load the file as an RDD of lines and choose elements at random
# from this array.

# context initialization
sc = SparkContext()

# read the input file line by line
text_file = sc.textFile('hdfs:///datasets/productGraph/complete.json')

# Choose a random sample of the data
sampled_text = text_file.sample(withReplacement=False, fraction=0.01, seed=1)

# Save the sample to text
sampled_text.saveAsTextFile('hdfs:///user/cyu/sample0.01.json')
