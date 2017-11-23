from pyspark import SparkContext

# context initialization
sc = SparkContext()

# read the input file line by line
text_file = sc.textFile('hdfs:///datasets/productGraph/complete.json')
#text_file = sc.textFile('hdfs:///user/cyu/frankenstein.txt')

sampled_text = text_file.sample(withReplacement=False, fraction=0.01, seed=1)

sampled_text.saveAsTextFile('hdfs:///user/cyu/sample0.01.json')
#sampled_text.saveAsTextFile('hdfs:///user/cyu/frankenstein_sampled.txt')


