# Databricks notebook source
# MAGIC %md
# MAGIC #Mount the S3 Bucket containing data from Kafka
# MAGIC 1. Get the AWS authentication key file
# MAGIC 2. Extract the key values
# MAGIC 3. Mount the S3 bucket
# MAGIC 4. Create 3 dataframes from the 3 locations in the mounted bucket
# MAGIC 5. Unmount the S3 bucket
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Authentication File

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables")
#dbutils.fs.ls("/user/hive/warehouse")

# COMMAND ----------

# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# Specify file type to be csv
file_type = "csv"
# Indicates file has first row as the header
first_row_is_header = "true"
# Indicates file has comma as the delimeter
delimiter = ","
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/FileStore/tables/authentication_credentials.csv")
#.load("/user/hive/warehouse/authentication_credentials_3_csv")#

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")


# COMMAND ----------

# MAGIC %md
# MAGIC ##Extract Access Keys

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secret key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# MAGIC %md
# MAGIC #Mount the Bucket

# COMMAND ----------

# AWS S3 bucket name
AWS_S3_BUCKET = "user-129a67850695-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/user-129a67850695-bucket"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)
display(dbutils.fs.ls("/mnt/user-129a67850695-bucket/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Pin data to a Dataframe

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/user-129a67850695-bucket/topics/129a67850695.pin/partition=0/*.json" 
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
# MAGIC ##Read Geo data to a Dataframe

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/user-129a67850695-bucket/topics/129a67850695.geo/partition=0/*.json" 
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
# MAGIC ##Read User data to a Dataframe

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/user-129a67850695-bucket/topics/129a67850695.pin/partition=0/*.json" 
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
# MAGIC ## Copy Dataframes to Global Temporary Views

# COMMAND ----------

df_pin.createOrReplaceGlobalTempView("df_pin")
df_geo.createOrReplaceGlobalTempView("df_geo")
df_user.createOrReplaceGlobalTempView("df_user")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Unmount the Bucket

# COMMAND ----------

dbutils.fs.unmount("/mnt/user-129a67850695-bucket")

# COMMAND ----------


