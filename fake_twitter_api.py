import json
from kafka import SimpleClient, SimpleProducer
import time
import random
import argparse
import requests
import os
import datetime


def download_file_from_google_drive(id_, destination_):
    url = "https://docs.google.com/uc?export=download"

    session = requests.Session()

    response = session.get(url, params={'id': id_}, stream=True)
    token = get_confirm_token(response)

    if token:
        params = {'id': id_, 'confirm': token}
        response = session.get(url, params=params, stream=True)

    save_response_content(response, destination_)


def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value

    return None


def save_response_content(response, destination_):
    chunk_size = 32768

    with open(destination_, "wb") as f:
        for chunk in response.iter_content(chunk_size):
            if chunk:
                # filter out keep-alive new chunks
                f.write(chunk)


if __name__ == '__main__':

    # getting the path to the json file
    # defining an argument parser to take the number of Tweets to stream,
    # the path to the credentials file,
    # the subjects to stream
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument('--count', type=int, default=100)
    argument_parser.add_argument('--path', type=str, default='./tweets.json')
    argument_parser.add_argument('--hosts', type=str, default='localhost:9092')
    argument_parser.add_argument('--topic', type=str, default='trump')
    argument_parser.add_argument('--verbose', action='store_true')

    # parsing arguments
    arguments = argument_parser.parse_args()
    kafka_hosts = arguments.hosts
    tweet_count = arguments.count
    topic = arguments.topic
    verbose = arguments.verbose

    # getting the path to the json file containing tweets
    path_to_tweets = arguments.path
    # getting the parent folder
    path_to_parent_folder = '/'.join(path_to_tweets.split('/')[:-1])

    # checking that the file exists
    if path_to_tweets.split('/')[-1] not in os.listdir(path_to_parent_folder):

        print('starting dowloading data')

        # downloading the data from Google Drive
        file_id = '1sOXurkOh48nT7AuuTqN80jf05D13WFld'
        destination = path_to_tweets
        download_file_from_google_drive(file_id, destination)

        print('downloading ended')

    # creating a kafka client
    kafka_client = SimpleClient(hosts=kafka_hosts)

    # creating a kafka producer
    kafka_producer = SimpleProducer(client=kafka_client)

    # opening the file containing the tweets
    with open(path_to_tweets, 'r') as file:
        tweets = json.load(file)
    number_of_available_tweets = len(tweets)

    print('emitting {} random tweets'.format(tweet_count))
    for i in range(1, 1 + tweet_count):
        tweet_index = random.randint(0, number_of_available_tweets-1)
        tweet = tweets[tweet_index]
        time.sleep(random.uniform(0, 1)/10)

        kafka_producer.send_messages('trump', str(tweet).encode('utf-8'))

        if verbose:
            print('\nTweet:\t{}/{}'.format(i, tweet_count))
            print('Author:\t', tweet['user']['screen_name'])
            print('Content:', tweet['text'])
        elif i % 100 == 0:
            print('{} - emitting tweet number {} over {}'.format(datetime.datetime.now(), i, tweet_count))

