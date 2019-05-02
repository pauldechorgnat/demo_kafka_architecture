from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.mllib.linalg import SparseVector
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.classification import StreamingLogisticRegressionWithSGD
import json
import re
import datetime
from happybase import Connection
import ast
import argparse
import os

time_reference = datetime.datetime.strptime('01/01/2030', '%d/%m/%Y')


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


def put_prediction_data_into_hbase(rdd):
    """
    functions to store data into hbase table
    """
    # collecting the results
    results = rdd.collect()
    # computing the exact time: this will serve as the row id
    date = (time_reference - datetime.datetime.now()).total_seconds()

    # making connection to the right
    connection = Connection(host='localhost', port=9090, autoconnect=True)

    table = connection.table(name='predictions_tweets_table')
    if len(results) != 0:
        table.delete(row='latest')

    for data in results:
        if data[0] == 0:
            table.put(row=str(date), data={'tweet_count:negative': str(data[1])})
            # 'tweet_count:now':  str(datetime.datetime.now())})
            # table.put(row='latest', data={'tweet_count:neg': str(data[1])})
        else:
            table.put(row=str(date), data={'tweet_count:positive': str(data[1])})
            # 'tweet_count:now':  str(datetime.datetime.now())})
            # table.put(row='latest', data={'tweet_count:pos': str(data[1])})

    connection.close()


def put_text_data_into_hbase(rdd):
    """
        functions to store data into hbase table
        """
    # collecting the results
    results = rdd.collect()

    # computing the exact time: this will serve as the row id
    now = datetime.datetime.now()
    date = (time_reference - now).total_seconds()

    # making connection to the right
    connection = Connection(host='localhost', port=9090, autoconnect=True)

    table = connection.table(name='text_tweets_table')

    for tweet in results[-5:]:
        table.put(row=str(date),
                  data={
                      'metadata:publication_date': tweet['created_at'],
                      'metadata:author': tweet['user']['screen_name'],
                      'text_data:text': tweet['text']
                  })

    connection.close()


def put_word_count_data_into_h_base(rdd):
    results = rdd.collect()
    now = datetime.datetime.now()
    date = (time_reference - now).total_seconds()
    if len(results) != 0:
        sorted_results = sorted(results, key=lambda x: x[1], reverse=True)[:100]
        connection = Connection(host='localhost', port=9090, autoconnect=True)
        table = connection.table(name='word_count_tweets_table')
        data_to_insert = {'word_count:{}'.format(word): str(count) for word, count in sorted_results}
        table.delete(row='latest')
        table.put(row='latest',
                  data=data_to_insert)
        table.put(row=str(date), data=data_to_insert)
        connection.close()


if __name__ == '__main__':

    # defining an argument parser
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument('--path', default='/home/hduser/demo_kafka_architecture')
    argument_parser.add_argument('--model-path', default='/home/hduser/demo_kafka_architecture/model.json', dest='model_path')

    # parsing arguments
    arguments = argument_parser.parse_args()
    data_directory = arguments.path
    model_path = arguments.model_path

    # defining paths
    data_path = os.path.join(data_directory, 'sample.csv')
    common_words_path = os.path.join(data_directory, 'most_used_words.csv')

    reference_table_ = create_common_words_reference_table(path=common_words_path)

    # creating a SparkContext object
    sc = SparkContext.getOrCreate()
    # setting the log level to avoid printing logs in the console
    sc.setLogLevel("ERROR")
    # creating a Spark Streaming Context
    ssc = StreamingContext(sparkContext=sc, batchDuration=1)

    # setting up a model
    lr = StreamingLogisticRegressionWithSGD()
    # loading the pre-trained parameters
    parameters = json.load(open(model_path, 'r'))
    # assigning the pre-trained parameters to the logistic regression
    lr.setInitialWeights(parameters['weights'])

    # opening the stream
    kafkaStream = KafkaUtils.createDirectStream(ssc=ssc,
                                                topics=['trump'],
                                                kafkaParams={"metadata.broker.list": 'localhost:9092'})

    # getting only the useful information
    dfs = kafkaStream.map(lambda stream: stream[1])
    # parsing data into a dictionary
    dfs = dfs.map(lambda raw: ast.literal_eval(raw))  # ast.literal_eval(raw.decode('utf-8')))
    # filtering data on tweets that are not in English
    dfs = dfs.filter(lambda dictionary: dictionary.get('lang', '') == 'en')
    # filtering data on tweets that contain text
    # this part is a security against empty data
    dfs = dfs.filter(lambda dictionary: dictionary.get('text', '') != '')
    # computing the word count
    word_re = re.compile('\\w+')
    dfs.map(lambda dictionary: dictionary.get('text', '')).\
        flatMap(lambda text: word_re.findall(text.lower())).\
        map(lambda word: (word, 1)). \
        reduceByKey(lambda x, y: x + y). \
        foreachRDD(put_word_count_data_into_h_base)

    # storing the text data
    dfs.foreachRDD(put_text_data_into_hbase)

    # changing words into tokens
    dfs = dfs.map(lambda dictionary: create_twitter_placeholder(text=dictionary.get('text', ''))).\
        map(tokenize).\
        map(lambda tokens: compute_term_frequency(tokens=tokens, reference_table=reference_table_))

    # making predictions using the logistic regression
    dfs_predictions = lr.predictOn(dfs)
    # preparing data to count positive and negative predictions
    dfs_predictions = dfs_predictions.map(lambda x: (x, 1))
    # computing positive and negative predictions counts
    dfs_predictions = dfs_predictions.reduceByKey(lambda x, y: x + y)

    # printing the results in the console
    dfs_predictions.pprint()

    # saving data into HBase
    dfs_predictions.foreachRDD(put_prediction_data_into_hbase)

    # starting streaming
    ssc.start()
    ssc.awaitTermination()

