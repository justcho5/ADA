import nltk
from nltk.corpus import stopwords
from nltk.tag import PerceptronTagger
from nltk.util import bigrams
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import ArrayType, StringType

DATA_PATH = 'hdfs:///user/mrizzo/filtered_df'
BIGRAMS_PATH = 'hdfs:///user/mrizzo/bigrams_df'


def lemmatize(wn, word, pos='N'):
    """
    Reduce a word to its base form using the WordNet corpus. Taken from nltk.
    :param wn: An instance of the WordNet corpus
    :param word: The word to lemmatize
    :param pos: The part-of-speech tag of word. Tags are in the format returned
    by nltk.pos_tag
    :return: The lemmatized word
    """
    pos = get_wordnet_pos(wn, pos)
    lemmas = wn._morphy(word, pos)
    return min(lemmas, key=len) if lemmas else word


def get_wordnet_pos(wn, treebank_tag):
    """
    Adapted from the tutorial on text processing
    Cf. https://stackoverflow.com/questions/15586721/wordnet-lemmatization-and-pos-tagging-in-python
    :param wn: An instance of the WordNet corpus
    :param treebank_tag: a tag from nltk.pos_tag treebank
    """

    if treebank_tag.startswith('J'):
        return wn.ADJ
    elif treebank_tag.startswith('V'):
        return wn.VERB
    elif treebank_tag.startswith('N'):
        return wn.NOUN
    elif treebank_tag.startswith('R'):
        return wn.ADV
    else:
        return wn.NOUN


def remove_stops(tokens, stops):
    """
    Remove stop words from an iterable of tokens
    :param tokens: An iterable of (token, part of speech tag) pairs
    :param stops: A list of stop words to remove, as a Spark broadcast variable
    :return: A list of all pairs in tokens where the token does not appear in
    stops
    """
    return [(token, pos) for token, pos in tokens if token not in stops.value]


def lemmatize_tagged_tokens(tagged_tokens, wn):
    """
    Lemmatize an iterable of tokens (extract their base form)
    :param tagged_tokens: An iterable of (token, part of speech tag) pairs.
    Tags are in the format returned by nltk.pos_tag
    :param wn: An instance of the WordNet corpus as a Spark broadcast variable
    :return: A list of strings containing the base forms of the tagged tokens
    """
    return [
        lemmatize(wn.value, token, pos=pos).lower()
        for token, pos in tagged_tokens
    ]


def main():
    """
    Process the tokenized text of each review by adding a part-of-speech tag,
    removing stop words, performing lemmatization, and finally making bigrams.
    These bigrams will be used to classify reviews as incentivized or
    non-incentivized.
    """
    # NLTK corpora are not present on the other nodes of the cluster,
    # we can either load them into memory on the master node or send the zip
    # file to the other nodes with sc.addFile(). The first approach worked so
    # we went with that. This also avoids using NLTK's lazy loader which ran into
    # infinite recursion after being unpickled on the slave nodes by Spark.
    root = nltk.data.find('corpora/omw')
    reader = nltk.corpus.CorpusReader(root, r'.*/wn-data-.*\.tab',
                                      encoding='utf8')
    # WordNet corpus for lemmatization
    wn = nltk.corpus.reader.WordNetCorpusReader(
        nltk.data.find('corpora/wordnet'), reader)

    sc = SparkContext()
    # Add the NLTK library
    sc.addPyFile('nltk.zip')

    sqlContext = SQLContext(sc)
    sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

    df = sqlContext.read.parquet(DATA_PATH)

    # List of English stop words, use set for better search performance
    stops = sc.broadcast(set(stopwords.words('english')))
    wn = sc.broadcast(wn)
    # NLTK's default part-of-speech tagger
    tagger = sc.broadcast(PerceptronTagger())

    # PoS tagging
    tag = UserDefinedFunction(
        lambda t: tagger.value.tag(t),
        ArrayType(ArrayType(StringType()))
    )

    # Remove stop words
    remstops = UserDefinedFunction(
        lambda t: remove_stops(t, stops),
        ArrayType(ArrayType(StringType()))
    )

    # Lemmatize
    lem = UserDefinedFunction(
        lambda t: lemmatize_tagged_tokens(t, wn),
        ArrayType(StringType())
    )

    # Make bigrams
    mk_bg = UserDefinedFunction(lambda t: list(bigrams(t)),
                                ArrayType(ArrayType(StringType())))

    # One function at a time or Spark complains about UDFs
    # not being callable
    df = df.withColumn('bg', tag(df.tokenized_text))
    df = df.withColumn('bg', remstops(df.bg))
    df = df.withColumn('bg', lem(df.bg))
    df = df.withColumn('bg', mk_bg(df.bg))

    # Save the results to HDFS
    df.write.mode('overwrite').parquet(BIGRAMS_PATH)


if __name__ == '__main__':
    main()
