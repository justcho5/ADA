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
    pos = get_wordnet_pos(wn, pos)
    lemmas = wn._morphy(word, pos)
    return min(lemmas, key=len) if lemmas else word

def get_wordnet_pos(wn, treebank_tag):
    """
    Cf. https://stackoverflow.com/questions/15586721/wordnet-lemmatization-and-pos-tagging-in-python
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
    return [(token, pos) for token, pos in tokens if token not in stops.value]


def lemmatize_tagged_tokens(tagged_tokens, wn):
    return [
        lemmatize(wn.value, token, pos=pos).lower()
        for token, pos in tagged_tokens
    ]


def main():
    # Forces nltk to load the wordnet corpus here, do not remove since the
    # corpus is not on the other nodes of the cluster
    root = nltk.data.find('corpora/omw')
    reader = nltk.corpus.CorpusReader(root, r'.*/wn-data-.*\.tab',
                                      encoding='utf8')
    wn = nltk.corpus.reader.WordNetCorpusReader(
        nltk.data.find('corpora/wordnet'), reader)

    sc = SparkContext()
    sc.addPyFile('nltk.zip')

    sqlContext = SQLContext(sc)
    sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

    df = sqlContext.read.parquet(DATA_PATH)

    stops = sc.broadcast(set(stopwords.words('english')))
    wn = sc.broadcast(wn)
    tagger = sc.broadcast(PerceptronTagger())

    tag = UserDefinedFunction(
        lambda t: tagger.value.tag(t),
        ArrayType(ArrayType(StringType()))
    )

    remstops = UserDefinedFunction(
        lambda t: remove_stops(t, stops),
        ArrayType(ArrayType(StringType()))
    )

    lem = UserDefinedFunction(
        lambda t: lemmatize_tagged_tokens(t, wn),
        ArrayType(StringType())
    )

    mk_bg = UserDefinedFunction(lambda t: list(bigrams(t)),
                                ArrayType(ArrayType(StringType())))

    # One function at a time or Spark complains about UDFs
    # not being callable
    df = df.withColumn('bg', tag(df.tokenized_text))
    df = df.withColumn('bg', remstops(df.bg))
    df = df.withColumn('bg', lem(df.bg))
    df = df.withColumn('bg', mk_bg(df.bg))

    df.write.mode('overwrite').parquet(BIGRAMS_PATH)


if __name__ == '__main__':
    main()
