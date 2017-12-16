from pyspark import SparkContext
from nltk.sentiment.vader import SentimentIntensityAnalyzer


def main():
    """
    Short test of the VADER sentiment classifier on Spark
    Adapted from https://github.com/cjhutto/vaderSentiment/blob/master/README.rst#python-code-example
    """
    sc = SparkContext()
    sc.addPyFile('nltk.zip')

    analyzer = sc.broadcast(SentimentIntensityAnalyzer())

    rdd = sc.parallelize([
        "VADER is smart, handsome, and funny.",  # positive sentence example
        "VADER is not smart, handsome, nor funny.",  # negation sentence example
        "VADER is smart, handsome, and funny!",  # punctuation emphasis handled correctly (sentiment intensity adjusted)
        "VADER is very smart, handsome, and funny.",  # booster words handled correctly (sentiment intensity adjusted)
        "VADER is VERY SMART, handsome, and FUNNY.",  # emphasis for ALLCAPS handled
        "VADER is VERY SMART, handsome, and FUNNY!!!",  # combination of signals - VADER appropriately adjusts intensity
        "VADER is VERY SMART, uber handsome, and FRIGGIN FUNNY!!!", # booster words & punctuation make this close to ceiling for score
        "The book was good.",  # positive sentence
        "The book was kind of good.",  # qualified positive sentence is handled correctly (intensity adjusted)
        "The plot was good, but the characters are uncompelling and the dialog is not great.", # mixed negation sentence
        "At least it isn't a horrible book.",  # negated negative sentence with contraction
        "Make sure you :) or :D today!",  # emoticons handled
        "Today SUX!",  # negative slang with capitalization emphasis
        "Today only kinda sux! But I'll get by, lol" # mixed sentiment example with slang and constrastive conjunction "but"
    ])

    print(rdd.map(analyzer.value.polarity_scores).collect())


if __name__ == '__main__':
    main()
