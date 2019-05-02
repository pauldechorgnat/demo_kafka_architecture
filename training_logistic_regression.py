from pyspark import SparkContext
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD
import re
import argparse
import os
import pprint
import json


def create_twitter_placeholder(text,
                               pseudo_regex='\\@[a-zA-Z0-9_]*',
                               link_regex='http(s)?://[a-zA-Z0-9./\\-]*',
                               hash_tag_regex='\\#[a-zA-Z0-9_]*',
                               rt_regex='rt:'):
    """
    replaces tweet specific objects by tokens
    :param text: text of the tweet
    :param pseudo_regex: regular expression for the pseudo
    :param link_regex: regular expression for the web site links
    :param hash_tag_regex: regular expression for the hash tags
    :param rt_regex: regular expression for the re-tweet
    :return: a transformed string
    """
    # replacing re-tweets
    text = re.sub(pattern=rt_regex, string=text, repl='retweetplaceholder')
    # replacing pseudos
    text = re.sub(pattern=pseudo_regex, string=text, repl='pseudoplaceholder')
    # replacing hash tags
    text = re.sub(pattern=hash_tag_regex, string=text, repl='hashtagplaceholder')
    # replacing links
    text = re.sub(pattern=link_regex, string=text, repl='linkplaceholder')

    return text


def tokenize(text, word_regex='\\w+'):
    """
    takes a text and return a list of tokens
    :param text: text to tokenize
    :param word_regex: regular expression used to retrieve words
    :return: a list of tokens
    """
    tokens = re.findall(pattern=word_regex, string=text.lower())
    return tokens


def create_common_words_reference_table(path):
    """
    returns a dictionary with tokens as keys and indices as values
    :param path: path to a csv file containing most common words to keep
    :return: a dictionary
    """
    # loading the most common words
    common_words = open(path).read().split('\n')

    # adding our placeholders
    common_words.extend(['pseudoplaceholder', 'linkplaceholder', 'retweetplaceholder', 'hashtagplaceholder'])

    # creating the reference table
    # the reference table is a dictionary with token as keys and indices as values
    reference_table = {token: index for index, token in enumerate(common_words)}

    return reference_table


def compute_term_frequency(tokens, reference_table):
    """
    function used to compute term frequency sparse vector
    """
    hash_table = {}
    # running through the tokens
    for token in tokens:
        # if the token is indeed among those we want to keep
        if token in reference_table.keys():
            # updating the frequency table
            hash_table[reference_table[token]] = hash_table.get(reference_table[token], 0) + 1
    # returning a Sparse vector object
    sparse_vector = SparseVector(len(reference_table), hash_table)
    return sparse_vector


if __name__ == '__main__':

    # defining an argument parser
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument('--path', default='/home/hduser/demo_kafka_architecture')
    argument_parser.add_argument('--output', default='/home/hduser/demo_kafka_architecture/model.json')

    # parsing arguments
    arguments = argument_parser.parse_args()
    data_directory = arguments.path
    output_path = arguments.output

    # defining paths
    data_path = os.path.join(data_directory, 'sample.csv')
    common_words_path = os.path.join(data_directory, 'most_used_words.csv')

    reference_table_ = create_common_words_reference_table(path=common_words_path)
    # directory_path = 'file:///home/hduser/twitter_data'
    # file_path = '/data/twitter_data/z_sample.csv

    # create a spark context
    spark_context = SparkContext.getOrCreate()
    spark_context.setLogLevel('ERROR')

    # reading data from the text file
    rdd = spark_context.textFile(data_path)

    # parsing data to get a dictionary
    rdd = rdd.map(lambda raw: raw.split(',', 1)). \
        filter(lambda tup: tup[0] != 'sentiment'). \
        map(lambda tup: {'sentiment': int(tup[0]), 'text': tup[1]})

    # replacing the twitter specific tokens by placeholders
    rdd = rdd.map(lambda dictionary: {
        'sentiment': dictionary['sentiment'],
        'text': create_twitter_placeholder(dictionary['text'])
    })

    # tokenizing the words
    rdd = rdd.map(lambda dictionary: {
        'sentiment': dictionary['sentiment'],
        'tokens': tokenize(dictionary['text'])
    })

    # creating the numerical features
    rdd = rdd.map(lambda dictionary: {
        'sentiment': dictionary['sentiment'],
        'features': compute_term_frequency(dictionary['tokens'], reference_table=reference_table_)
    })

    # formatting the data for the logistic regression
    rdd_features = rdd.map(lambda dictionary: LabeledPoint(label=dictionary['sentiment'],
                                                           features=dictionary['features']))

    # splitting the data into a train and a test set
    rdd_train, rdd_test = rdd_features.randomSplit(weights=[.8, .2])

    # instantiating a logistic regression
    logistic_regression = LogisticRegressionWithSGD()
    trained_logistic_regression = logistic_regression.train(rdd_train)

    predictions_on_test = trained_logistic_regression.predict(rdd_test.map(lambda point: point.features))

    def confusion_matrix(x):

        if x[0] == x[1]:
            beginning = 'True '
        else:
            beginning = 'False '
        if x[0] == 1:
            end = 'Positive'
        else:
            end = 'Negative'

        return beginning + end

    results = predictions_on_test.zip(rdd_test).\
        map(lambda tup: (tup[0], int(tup[1].label))).\
        map(confusion_matrix).\
        map(lambda x: (x, 1)).\
        reduceByKey(lambda x, y: x + y)

    pprint.pprint(dict(results.collect()))

    # storing the parameters in a json file
    trained_parameters = {
        'weights': trained_logistic_regression.weights.toArray().tolist(),
        'intercept': trained_logistic_regression.intercept
    }

    with open(output_path, 'w') as model_file:
        json.dump(trained_parameters, fp=model_file)

