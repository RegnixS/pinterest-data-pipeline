# Databricks notebook source
# MAGIC %md
# MAGIC #Mount the S3 Bucket containing data from Kafka
# MAGIC 1. Get the AWS authentication key file 
# MAGIC 2. Mount the S3 bucket
# MAGIC 3. Define read_from_S3 function
# MAGIC 4. Create 3 dataframes from the 3 locations in the mounted bucket
# MAGIC 5. Copy Dataframes to Global Temporary Views
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get authentication File

# COMMAND ----------

# MAGIC %run "./Get Authentication Keys"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Mount the bucket

# COMMAND ----------

# AWS S3 bucket name
AWS_S3_BUCKET = "user-129a67850695-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/user-129a67850695-bucket"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
try:
    dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)
except:
# If error try unmounting it first
    dbutils.fs.unmount(MOUNT_NAME)
    dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

display(dbutils.fs.ls("/mnt/user-129a67850695-bucket/"))

# COMMAND ----------

# We need to turn off delta format check before trying to read the json files because of a change to Databricks configuration
spark.conf.set("spark.databricks.delta.formatCheck.enabled", False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Define read_from_S3 function

# COMMAND ----------

def read_from_S3(file_location):
    '''
    This function reads json data from an S3 bucket and returns a dataframe.

    Args:
        file_location (string) : The mounted DBFS file location mapped to where the json data is held in the bucket.

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
# MAGIC ##Read pin data to a dataframe

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/user-129a67850695-bucket/topics/129a67850695.pin/partition=0/*.json"

# Read from S3
df_pin = read_from_S3(file_location)

# Display Spark dataframe to check its content
display(df_pin)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read geo data to a dataframe

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/user-129a67850695-bucket/topics/129a67850695.geo/partition=0/*.json" 

# Read from S3
df_geo = read_from_S3(file_location)

# Display Spark dataframe to check its content
display(df_geo)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read user data to a dataframe

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/user-129a67850695-bucket/topics/129a67850695.user/partition=0/*.json" 

# Read from S3
df_user = read_from_S3(file_location)

# Display Spark dataframe to check its content
display(df_user)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy dataframes to global temporary views

# COMMAND ----------

df_pin.createOrReplaceGlobalTempView("gtv_129a67850695_pin")
df_geo.createOrReplaceGlobalTempView("gtv_129a67850695_geo")
df_user.createOrReplaceGlobalTempView("gtv_129a67850695_user")
