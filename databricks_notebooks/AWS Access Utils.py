# Databricks notebook source
# MAGIC %md
# MAGIC ##Get AWS Authentication Keys

# COMMAND ----------

# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secret key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Define bucket name

# COMMAND ----------

# AWS S3 bucket name
AWS_S3_BUCKET = "user-129a67850695-bucket"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Define function read_from_S3

# COMMAND ----------

def read_from_S3(file_location):
    '''
    This function reads json data from an S3 bucket and returns a dataframe.

    Args:
        file_location (string) : The file location where the json data is held in the bucket.

    Returns:
        pyspark.sql.DataFrame : A DataFrame of the data.
    '''
    file_type = "json"
    # Ask Spark to infer the schema
    infer_schema = "true"
    # Read in JSONs from mounted S3 bucket
    df_out = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .load(file_location)

    return df_out

# COMMAND ----------

# MAGIC %md
# MAGIC ##Define function read_stream

# COMMAND ----------

def read_stream(stream_name, schema):
    '''
    This function reads data from a Kinesis stream, extracts the columns from the "data" column and returns a datframe with the schema provided.

    Args:
        stream_name (string) : The name of the stream to read from.
        schema (string) : A string containing the schema of the output dataframe.

    Returns:
        pyspark.sql.DataFrame : A DataFrame with the provided schema.
    '''
    from pyspark.sql.functions import col, from_json
    
    # Read in the Kinesis stream to a dataframe
    df_kinesis = spark.readStream \
        .format('kinesis') \
        .option('streamName', stream_name) \
        .option('initialPosition','earliest') \
        .option('region','us-east-1') \
        .option('awsAccessKey', ACCESS_KEY) \
        .option('awsSecretKey', SECRET_KEY) \
        .load()

    # Create a new dataframe containing the columns exploded from the "data" column
    df_out = df_kinesis.select(from_json(col("data").cast("string"), schema).alias("data")) \
        .select("data.*")
    
    return df_out

# COMMAND ----------

# MAGIC %md
# MAGIC ##Define function write_stream

# COMMAND ----------

def write_stream(table_name, df_in):
    '''
    This function writes data from a kinesis stream to a delta table.

    Args:
        table_name (string) : The name of the delta table to write to.
        df_in (pyspark.sql.DataFrame) : The dataframe to be written to the table.
    '''
    # Write the cleaned dataframe to a Delta Table 
    df_in.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"/tmp/kinesis/{table_name}_checkpoints/") \
        .table(table_name)
