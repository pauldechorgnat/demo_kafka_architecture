# Demo of a Kafka Architecture

_This repo contains the code used in the Data Engineering training at [DataScientest](https://datascientest.com/).

## Description of the demo
This demo intends to create a Kappa architecture:
- Tweets are streamed using the Twitter API.
- They are emitted to Kafka.
- They are retrieved from Kafka using Spark Streaming.
- We classify the sentiment and stored also some information on those tweets in HBase
- Finally, we can show the outcome of this application using a Dash application


## Description of the files
### Kafka producers
- `true_twitter_api.py`: code to have a kafka producer emitting live tweets
- `fake_twitter_api.py`: code meant to mimic the behaviour of the previous if you do not have any credentials

### Spark files
- `training_logistic_regression.py`: code used to train a sentiment analysis logistic regression with Spark ML
- `model.json`: contains the weights of the logistic regression
- `streaming_data.py`: code used to make predictions and process tweets in real-time. makes the link with the serving database 
- `spark-streaming.jar`: jar file used to link spark streaming and kafka 

### Dash 
- `dash_app.py`: code used to launch a Dash application on the localhost:5000

## Requirements
### Softwares
This works with the following softwares installed:
- Python3
- Hadoop 2.7.3
- Spark 2.4.0
- Zookeeper 3.4.13
- Kafka 2.1.0
- HBase 2.1.3

### Packages
The packages in `requirements.txt` should be installed.

### Ports
This part lists the port that are used. _You can change them but those worked for me._
- Hadoop is installed in pseudo-distributed mode: namenode should be on port 9000
- Spark is installed in stand alone mode (ie using itself as cluster manager): master should be running on port 8080
- Zookeeper should be running on port 2181 for Kafka and 2000 for Hbase
- Hbase HMaster is relying on HDFS and running on port 16000
- A Thrift Server serving HBase should be up and running on port 9090
- We will launch a Kafka server on port 9092 (There should be a topic named `trump`)
- By default the Dash application will be launched on port 5000

By calling `jps`, you should be able to see something similar to this
```22528 ThriftServer
4645 HRegionServer
10279 JobHistoryServer
3049 NameNode
3498 SecondaryNameNode
3855 NodeManager
1583 QuorumPeerMain
2704 Master
3666 ResourceManager
3251 DataNode
4533 HMaster
4476 HQuorumPeer
21214 Jps
```

## Strreaming Commands
### Starting Kafka server

```$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties```

### Starting the emission of tweets
To use the Twitter API with your own credentials:

```python3 true_twitter_api.py --count <number_of_tweets_to_emit> --path <path_to_twitter_credentials>```

To fake the behaviour of the API:

```python3 true_twitter_api.py --count <number_of_tweets_to_emit>```

### Processing tweets 

```spark-submit --jars spark-streaming.jar streaming_data.py```

### Launching the dashboard

```python3 dash_app.py```


