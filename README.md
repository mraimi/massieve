# Massieve

Massieve is an application that aims to detect anomalous network traffic
in real-time at massive scale.

## The Massieve pipeline
- Kafka-Python produces traffic messages to Kafka
- Spark Stremaing receives Kafka messages and passes them to MLlib
- MLlib writes back to the model and publishes newly classified records to Redis
- Flask front end dynamically subscribes to Redis channels


## Scale

Apache Kafka is used to ingest and distribute the stream of network 
traffic coming in. A Kafka-Python script simulates the data coming in
and increased levels of traffic can be achieved by running multiple
threads of the producer with something like tmux. 

NOTE: The producer has a large memory footprint
as it loads a 700MB data record file in memory, chooses a random
record, and perturbs the data modestly.

## Building a model

Massieve leverages Apache Spark's streaming capabilities as well as 
an algorithm from Spark's Machine Learning library, MLlib. Spark 
Streaming takes care of ingesting the Kafka stream and distributing
it into RDDs (Resilient Distributed Datasets.) These RDDs are
then passed onto the an incremental k-means clustering algorithm.

Incremental k-means gets started by building a model on 
historical data to which it adds the new streaming records to 
update the model. 

## Classifying anomalies

As we build clusters we establish centroids, points that represents
the center of a given cluster. We can measure the distance between
a centroid and newly arriving points (Massieve uses Euclidian distance.)
If the distance exceeds some threshold we can declare the point
is anomalous.

## Establishing thresholds

Since each threshold may have a different relative distribution
of points Massieve allows for evolving thresholds by calculating 
them as a function of the standard deviation of distances from the 
centroid for any given cluster.

## Filtering/Viewing Traffic

From the front end (www.massieve.co) users are able to "subscribe" to different types of
traffic by selecting connection type and protocol. For example,
one could choose to see all incoming UDP traffic and whether each
looks normal or anomalous. One could also see all TCP connections
using the HTTP protocol. 











