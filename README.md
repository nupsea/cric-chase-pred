
# PyFlink Score Publisher

## Overview
The PyFlink Score Publisher is a tool designed to publish scores from a PyFlink job to a specified endpoint.
In this case it is used to publish match updates to a Kafka topic which is then used by a prediction web-service
to display the probability of a team winning a match.
It is built using Python and utilizes the Apache Flink framework for stream processing.


## Prerequisites
- Python 3.11 (or higher)
- Kafka cluster 
- pyflink==1.19.2 
- IDE of your choice 
- pipenv (optional but recommended)

## Setup

Download this repository and open it in your IDE.


Install Apache Flink 1.19.2 (preferably) and set up a standalone cluster.
You can follow the official [Flink documentation](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/deployment/cluster_setup/) for instructions on how to set up a standalone cluster.

Note the dependent Flink JAVA jars are already included in the `jars` directory.
Install the required Python packages using pipenv or pip:

In addition, follow the quickstart guide to run the model/streamlit app in for dependent prediction service.
(ipl_infer) streamlit run rt_app.py


```bash
pipenv install
```


## Run
You can start the Kafka cluster using the following commands:
```bash

cd docker/kafka
docker-compose up -d

```
Ensure, kafka is running and the topics `t20-deliveries` and `t20-model-input` are created in the kafka cluster.

Load the project into your PyCharm IDE and run the `stream_runner.py` file under src/exec to start the PyFlink job.

This will start the PyFlink job and it will begin to consume messages from the Kafka topic 
and publish scores to the kafka `t20-model-input` topic.

Run the `inn1_simulator.py` file under src/feed to start the simulator for the first innings.
This will start the simulator and it will begin to produce messages to the kafka topic `t20-deliveries`
and the PyFlink job will consume these messages and print out the target score which is persisted as 
a state in Flink to enrich Innings 2 messages.

Run the `inn2_simulator.py` file under src/feed to start the simulator for the second innings.
This will start the simulator and it will begin to produce messages to the kafka topic `t20-deliveries`
and the PyFlink job will consume these messages and publish the match updates to the kafka topic `t20-model-input`.

The rt_app.py streamlit app will consume the messages from the redis store and display the probability of a team winning a match.