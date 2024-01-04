# Databricks notebook source
# MAGIC %md
# MAGIC #Access the S3 Bucket containing data from Kafka
# MAGIC Because accessing S3 Buckets using Databricks filesystem mounts has been deprecated, this notebook implements the same functionality without creating a mount point. [See link.](https://docs.databricks.com/en/connect/storage/amazon-s3.html#deprecated-patterns-for-storing-and-accessing-data-from-databricks) 
# MAGIC 1. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/198588058359572/command/198588058359579">Get the AWS authentication key file</a>
# MAGIC 2. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/198588058359572/command/198588058359581">Extract the key values</a>
# MAGIC 3. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/198588058359572/command/198588058359583">Create 3 dataframes from the 3 locations in the bucket</a>
# MAGIC 4. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/198588058359572/command/198588058359592">Copy Dataframes to Global Temporary Views</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get authentication File

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

# MAGIC %md
# MAGIC ##Extract access keys

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secret key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# AWS S3 bucket name
AWS_S3_BUCKET = "user-129a67850695-bucket"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read pin data to a dataframe

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "s3n://{0}:{1}@{2}/topics/129a67850695.pin/partition=0/*.json".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_pin)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read geo data to a dataframe

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "s3n://{0}:{1}@{2}/topics/129a67850695.geo/partition=0/*.json".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_geo)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read user data to a dataframe

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "s3n://{0}:{1}@{2}/topics/129a67850695.user/partition=0/*.json".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_user)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy dataframes to global temporary views

# COMMAND ----------

df_pin.createOrReplaceGlobalTempView("df_pin")
df_geo.createOrReplaceGlobalTempView("df_geo")
df_user.createOrReplaceGlobalTempView("df_user")
