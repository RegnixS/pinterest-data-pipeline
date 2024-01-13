# Databricks notebook source
# MAGIC %md
# MAGIC #Access the S3 Bucket containing data from Kafka
# MAGIC Because accessing S3 Buckets using Databricks filesystem mounts has been deprecated, this notebook implements the same functionality without creating a mount point. [See link.](https://docs.databricks.com/en/connect/storage/amazon-s3.html#deprecated-patterns-for-storing-and-accessing-data-from-databricks) 
# MAGIC 1. Get the AWS authentication key file
# MAGIC 2. Create 3 dataframes from the 3 locations in the bucket
# MAGIC 3. Copy Dataframes to Global Temporary Views

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get authentication File

# COMMAND ----------

# MAGIC %run "./AWS Access Utils"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read pin data to a dataframe

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "s3n://{0}:{1}@{2}/topics/129a67850695.pin/partition=0/*.json".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)

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
file_location = "s3n://{0}:{1}@{2}/topics/129a67850695.geo/partition=0/*.json".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)

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
file_location = "s3n://{0}:{1}@{2}/topics/129a67850695.user/partition=0/*.json".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)

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
