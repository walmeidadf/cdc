# Change Data Catpture

Change Data Capture (CDC) is a data integration standard that allows the identification, capture and transmission of events that occurred in a transactional database.

## Overview

There are some possible approaches for CDC, here we will use the Debezium tool, an open source project that uses the Log-Base approach, that is, there is a specific connector for the data source in which it connects to the database to read changes recorded in the log.

Debezium has connectors for different data sources (MySQL, MongoDB, PostgreSQL, SQL Server, Oracle). Here we will start by showing the integration with PostgreSQL.

Once an insert, delete or update operation on a table being monitored is identified and captured, Debezium can transmit this information to a messaging infrastructure. It is quite common to do this integration with an Apache Kafka cluster through Apache Kafka Connect. Recently, I participated in two projects where they chose to use a cloud messaging platform, Amazon Kinesis and Azure Event Hubs. There are also connectors for Google Cloud Pub/Sub and other open source platforms such as Redis and Apache Pulsar.

In this project we will use a Kafka node operating in the Kafka Raft (kraft) format, that is, without depending on a Zookeeper cluster and with a node that is both controller and broker.

To process the change events that will be inserted into the Kafka cluster topics, we will use an Apache Spark Standalone node where we will submit a job written in PySpark and using the Structured Streaming approach. This job will read the messages written in some of the topics, process the information, perform some transformations and save the data in a Delta Lake database.

## Build with:

Lab environment:

![Architecture design of the Change Data Capture Lab](/image/lab_cdc.png "Componentes da solução de CDC")

- PostreSQL v15.3
- Debezium v2.3.1
- Apache Kafka v3.2.x
- Apache Spark 3.3.2
- JupyterLab
- Docker Compose 2.15

## Getting Started

This project serves as a stepping stone for learning about Change Data Capture (CDC).

It provides a running environment with sample data inserts and demonstrates how different components capture and transform changes.

Important Note: While the project functions well for training purposes, some configurations (particularly security-related settings) are simplified for ease of use and should not be adopted in production environments.

### Prerequisites:

This project uses Docker containers. Please ensure you have Docker and Docker Compose installed correctly.

You can find installation instructions in the official Docker documentation [here](https://docs.docker.com/compose/install/)

To confirm your installation is working, run the following command:

```sh
   $ docker compose version
   Docker Compose version v2.15.1
   ```

### Installation

1. Clone the repository
   ```sh
   git clone https://github.com/walmeidadf/cdc.git
   ```
2. Run the Docker Compose command in the project folder. to activate the containers with the systems..
   ```sh
   cd cdc/cdc_psql_kafka
   docker compose up
   ```
3. Optionally, add records for DNS file. If you are using Linux, this would be the `/etc/hosts` file.
   ```sh
   172.26.0.2       db_source
   172.26.0.5       kafka-1
   172.26.0.6       jupyter_spark
   ```
## Usage

To explore the entire environment, simply visit `http://localhost:9888` on the `jupyter_spark` machine to access the JupyterLab interface. Use the password `example` to log in (you can change this in the docker-compose.yaml file).

Within the file system navigation on Jupyter, head to the `work` folder. You'll find notebooks demonstrating how to test different aspects of the architecture. Each notebook includes comments to guide you.

For test stream operations, a script has been created.

To run the PySpark script that reads Kafka topics and writes data to Delta Lake, use the following command:

```
docker exec cdc_jupyter_spark sh -c "python3 ~/work/kafka-structured-streaming.py"
```

## Roadmap

- [ ] Add new data sources
    - [ ] MySQL
    - [ ] MongoDB


## Contact

Wesley Almeida - [@walmeidadf](https://twitter.com/your_username) - walmeida@gmail.com

Project Link: [https://github.com/walmeidadf/cdc](https://github.com/walmeidadf/cdc)

## Acknowledgments

A large part of this project has an invaluable contribution from my colleague Egon Rosa Pereira.

Some of the documentation pages, articles and posts that helped me develop this project:

* [Debezium connector for PostgreSQL](https://debezium.io/documentation/reference/1.9/connectors/postgresql.html)
* [Jupyter Notebook Python, Spark Stack](https://hub.docker.com/r/jupyter/pyspark-notebook)
