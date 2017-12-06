import json
import datetime

from pyspark import SparkContext, SQLContext

from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import RegexpTokenizer

OUTPUT_PATH = 'hdfs:///user/mrizzo/review_df'
DATA_PATH = 'hdfs:///datasets/productGraph/complete.json'

def add_day_month_year(row):
    dt = datetime.datetime.fromtimestamp(row['unixReviewTime'])

    row.update({'day': dt.day, 'month': dt.month, 'year': dt.year})
    return row


def add_sentiment_score(row, analyzer):
    score = analyzer.value.polarity_scores(row['reviewText'])

    row.update({
        'positive_score': score['pos'],
        'negative_score': score['neg'],
        'compound_score': score['compound'],
        'neutral_score':  score['neu']
    })
    return row


def tokenize_text(row, tokenizer):
    tok = tokenizer.value.tokenize(row['reviewText'])

    row.update({'tokenized_text': tok, 'word_count': len(tok)})
    return row


def add_helpfulness(row):
    helpful_pos = row['helpful'][0]
    helpful_tot = row['helpful'][1]
    helpful_ratio = helpful_pos / helpful_tot if helpful_tot else float("NaN")

    row.update({
        'helpful_pos': helpful_pos,
        'helpful_tot': helpful_tot,
        'helpful_ratio': helpful_ratio
    })
    return row


sc = SparkContext()
sc.addPyFile('nltk.zip')

sqlContext = SQLContext(sc)
sqlContext.setConf('spark.sql.parquet.compression.codec','snappy')

vader = sc.broadcast(SentimentIntensityAnalyzer())
tok = sc.broadcast(RegexpTokenizer(r'\w+'))

reviews = sc.textFile(DATA_PATH)\
    .map(json.loads)\
    .filter(lambda r: r['reviewText'] and r['unixReviewTime'])\
    .map(add_day_month_year)\
    .map(lambda r: add_sentiment_score(r, vader))\
    .map(lambda r: tokenize_text(r, tok))\
    .map(add_helpfulness)

df = sqlContext.createDataFrame(reviews)

df.write.mode('overwrite').parquet(OUTPUT_PATH)