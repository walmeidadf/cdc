from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from delta.tables import *
from collections import namedtuple

import os
import json

def main():
    """Main ETL script definition.

    :return: None
    """

    # Spark JAR packages
    jar_packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1",
                    "io.delta:delta-core_2.12:1.2.1"]

    # Spark config params
    spark_config = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    }

    # start Spark application and get Spark session and config
    spark = start_spark(
        app_name='LabCDC',
        jar_packages=jar_packages,
        spark_config=spark_config)

    # Create or replace table (data sink)
    location = "/delta_lake/customers"
    create_delta_table(spark, location)

    # Kafka config (data source)
    topic_name = "lab_cdc.inventory.customers"
    kafka_brokers = ["kafka-1:9092"]
    num_partitions = 3

    # infer Kafka schema
    topic_schema_value = infer_schema_json(spark, "value", kafka_brokers, topic_name, num_partitions)
    topic_schema_key = infer_schema_json(spark, "key", kafka_brokers, topic_name, num_partitions)
    TopicSchema = namedtuple('TopicSchema', ['key', 'value'])
    topic_schema = TopicSchema(key=topic_schema_key, value=topic_schema_value)

    # execute ETL pipeline
    data = extract_data(spark, kafka_brokers, topic_name)
    data_transformed = transform_data(data, topic_schema)
    squery = load_data(data_transformed, location)

    # Waits for the termination of this query, either by query.stop() or by an exception
    squery.awaitTermination()

    # terminate Spark application
    # spark.stop()
    return None

def extract_data(spark, kafka_brokers, topic_name):
    """Load data from Parquet file format.

    :param spark: Spark session object.
    :param kafka_brokers: List of kafka broker hosts
    :param topic_name: String ou regex of the kafka topic name
    :return: Spark DataFrame.
    """
    
    kafka_brokers = ",".join(kafka_brokers)

    df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", topic_name)
        .load())

    return df


def transform_data(df, topic_schema):
    """Transform original dataset.

    :param df: Input DataFrame.
    :param topic_schema: Json Schema for kafka topic messages.
    :return: Transformed DataFrame.
    """
    topic_schema_key = StructType.fromJson(json.loads(topic_schema.key))
    topic_schema_value = StructType.fromJson(json.loads(topic_schema.value))

    df_transformed = (
        df
        .withColumn("value", F.expr("string(value)"))
        .filter(F.col("value").isNotNull())
        .select(
            F.expr("offset as kafka_offset"),
            F.expr("timestamp as created_at"),
            F.expr("string(key) as kafka_key"),
            "value")
        .withColumn('value', F.from_json(F.col("value"), topic_schema_value))
        .withColumn('key_json', F.from_json(F.col("kafka_key"), topic_schema_key))
        .select('kafka_key', 
                'kafka_offset', 
                'value.payload.op', 
                'key_json.payload.id', 
                'value.payload.after.first_name', 
                'value.payload.after.last_name', 
                'value.payload.after.email',
                'created_at'))

    return df_transformed


def load_data(df, location):
    """Collect data locally and write to CSV.

    :param df: DataFrame to save.
    :return: None
    """
    checkpoint = location + "/_checkpoints/etl-from-kafka"
    return (
     df
     .writeStream
     .format("delta")
     .outputMode("append")
     .option("checkpointLocation", checkpoint)
     .start(location))

def create_delta_table(spark, location):
    """Create or replace table with path and add properties.

    :param location: folder to create or read DeltaLake table.
    :return: None
    """
    deltaTable = (
        DeltaTable.createOrReplace(spark)
        .addColumn("kafka_key", "STRING")
        .addColumn("kafka_offset", "BIGINT")
        .addColumn("op", "STRING")
        .addColumn("id", "BIGINT")
        .addColumn("first_name", "STRING")
        .addColumn("last_name", "STRING")
        .addColumn("email", "STRING")
        .addColumn("created_at", "TIMESTAMP")
        .addColumn("updated_at", "TIMESTAMP")
        .property("description", "table with customers data")
        .location(location)
        .execute())
    return None

def infer_schema_json(spark, column, kafka_brokers, topic_name, num_partitions):
    """infer schema of a dataframe from kafka and return the schema on json format.

    :param spark: spark session
    :param column: column that will infer the schema
    :param kafka_brokers: list of kafka broker hosts
    :param topic_name: string - the name of kafka topic
    :num_partitions: int - number of partitions of Kafka topic
    :return: Json string
    """
    # configure the offsets to read on kafka topic
    ending_offsets = {"{}".format(x):-1 for x in range(num_partitions)}
    ending_offsets = '{"'+topic_name+'":'+str(ending_offsets).replace("'", '"')+"}"

    kafka_brokers = ",".join(kafka_brokers)

    # read some messages from the topic
    tdf = (
        spark 
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", ending_offsets)
        .load())

    df_json = (
        # filter out empty values
        tdf.withColumn(column, F.expr("string({})".format(column)))
        .filter(F.col(column).isNotNull())
        # get latestecord
        .select("key", F.expr("struct({}) r".format(column)))
        .groupBy("key").agg(F.expr("max(r) r")) 
        .select("r.{}".format(column)))

    # decode the json values
    df_read = spark.read.json(df_json.rdd.map(lambda x: x[column]), multiLine=True)

    # drop corrupt records
    if "_corrupt_record" in df_read.columns:
        df_read = (df_read.filter(col("_corrupt_record").isNotNull()).drop("_corrupt_record"))

    # schema
    return df_read.schema.json()

def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[], spark_config={}):
    """Start Spark session, get Spark logger and load config files.

    Start a Spark session on the worker node and register the Spark
    application with the cluster. Note, that only the app_name argument
    will apply when this is called from a script sent to spark-submit.
    All other arguments exist solely for testing the script from within
    an interactive Python console.

    This function also looks for a file ending in 'config.json' that
    can be sent with the Spark job. If it is found, it is opened,
    the contents parsed (assuming it contains valid JSON for the ETL job
    configuration) into a dict of ETL job configuration parameters,
    which are returned as the last element in the tuple returned by
    this function. If the file cannot be found then the return tuple
    only contains the Spark session and Spark logger objects and None
    for config.

    The function checks the enclosing environment to see if it is being
    run from inside an interactive console session or from an
    environment which has a `DEBUG` environment variable set (e.g.
    setting `DEBUG=1` as an environment variable as part of a debug
    configuration within an IDE such as Visual Studio Code or PyCharm.
    In this scenario, the function uses all available function arguments
    to start a PySpark driver from the local PySpark package as opposed
    to using the spark-submit and Spark cluster defaults. This will also
    use local module imports, as opposed to those in the zip archive
    sent to spark via the --py-files flag in spark-submit.

    :param app_name: Name of Spark app.
    :param master: Cluster connection details (defaults to local[*]).
    :param jar_packages: List of Spark JAR package names.
    :param spark_config: Dictionary of config key-value pairs.
    :return: The Spark session.
    """

    # get Spark session factory
    spark_builder = (
        SparkSession
        .builder
        .appName(app_name))

    # create Spark JAR packages string
    spark_jars_packages = ','.join(list(jar_packages))
    spark_builder.config('spark.jars.packages', spark_jars_packages)

    # add other config params
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    # create session object
    spark_sess = spark_builder.getOrCreate()

    return spark_sess


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()