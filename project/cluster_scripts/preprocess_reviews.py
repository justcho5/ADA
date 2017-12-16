import json
import datetime

from pyspark import SparkContext, SQLContext

from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import RegexpTokenizer

OUTPUT_PATH = 'hdfs:///user/mrizzo/review_df'
DATA_PATH = 'hdfs:///datasets/productGraph/complete.json'


def add_day_month_year(row):
    """
    Add day of the month, month and year fields to each review
    :param row: A python dict containing data for a single review
    :return: A python dict containing the original data plus the added features
    """
    dt = datetime.datetime.fromtimestamp(row['unixReviewTime'])

    row.update({'day': dt.day, 'month': dt.month, 'year': dt.year})
    return row


def add_sentiment_score(row, analyzer):
    """
    Add sentiment scores to a review
    :param row: A python dict containing data for a single review
    :param analyzer: An instance of the VADER sentiment score analyzer as a
    Spark broadcast variable
    :return: A python dict containing the original data plus the added sentiment
    scores
    """
    score = analyzer.value.polarity_scores(row['reviewText'])

    # The VADER analyzer gives each document four scores: positive, negative,
    # neutral and compound
    row.update({
        'positive_score': score['pos'],
        'negative_score': score['neg'],
        'compound_score': score['compound'],
        'neutral_score': score['neu']
    })
    return row


def tokenize_text(row, tokenizer):
    """
    Use the given tokenizer to split the text of a review into tokens
    :param row: A python dict containing data for a single review
    :param tokenizer: A tokenizer as a Spark broadcast variable
    :return: A python dict containing the original data plus the tokenized text
    as an array of strings and the word count of the review
    """
    tok = tokenizer.value.tokenize(row['reviewText'])

    row.update({'tokenized_text': tok, 'word_count': len(tok)})
    return row


def add_helpfulness(row):
    """
    Add helpfulness scores and ratio as separate variables to a row
    :param row: A python dict containing data for a single review
    :return: A python dict containing the original data plus the number of users
    that rated this review as helpful, the total votes received by the review,
    and the ratio between the two. Reviews with no ratings have a NaN ratio
    """
    helpful_pos = row['helpful'][0]
    helpful_tot = row['helpful'][1]
    helpful_ratio = helpful_pos / helpful_tot if helpful_tot else float("NaN")

    row.update({
        'helpful_pos': helpful_pos,
        'helpful_tot': helpful_tot,
        'helpful_ratio': helpful_ratio
    })
    return row


def main():
    """
    Preprocess the file containing reviews by loading the JSON-encoded data,
    adding new features, creating a DataFrame and saving the results.
    """
    sc = SparkContext()
    # Add NLTK for the VADER sentiment analyzer and regexp tokenizer
    sc.addPyFile('nltk.zip')

    sqlContext = SQLContext(sc)
    sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

    # Instances of the sentiment score analyzer and tokenizer, mutation is not
    # needed so broadcasting is okay and should improve performance
    vader = sc.broadcast(SentimentIntensityAnalyzer())
    tok = sc.broadcast(RegexpTokenizer(r'\w+'))

    # Load the data as a RDD of strings, one per line, then process it.
    # Each line is a JSON object containing data for one review. Reviews that
    # do not contain a review time or review text are removed.
    reviews = sc.textFile(DATA_PATH) \
        .map(json.loads) \
        .filter(lambda r: 'unixReviewTime' in r and
                          'reviewText' in r and
                          r['reviewText'] and
                          r['unixReviewTime']) \
        .map(add_day_month_year) \
        .map(lambda r: add_sentiment_score(r, vader)) \
        .map(lambda r: tokenize_text(r, tok)) \
        .map(add_helpfulness)

    # Convert the result to a DataFrame
    df = sqlContext.createDataFrame(reviews)

    # Save it to HDFS
    df.write.mode('overwrite').parquet(OUTPUT_PATH)


if __name__ == '__main__':
    main()
