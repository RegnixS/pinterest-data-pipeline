# Databricks notebook source
# MAGIC %md
# MAGIC #Kinesis Streaming
# MAGIC 1. Get the AWS authentication key file
# MAGIC 2. Read the pin Kinesis stream
# MAGIC 3. Clean the pin data
# MAGIC 4. Save pin to a delta table
# MAGIC 5. Read the geo Kinesis stream
# MAGIC 6. Clean the geo data
# MAGIC 7. Save geo to a delta table
# MAGIC 8. Read the user Kinesis stream
# MAGIC 9. Clean the user data
# MAGIC 10. Save user to a delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get authentication File

# COMMAND ----------

# MAGIC %run "./AWS Access Utils"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read the pin Kinesis stream

# COMMAND ----------

# Define the structure of the array in the "data" column
schema = (
    "index long, unique_id string, title string, description string, poster_name string, " +
    "follower_count string, tag_list string, is_image_or_video string, " +
    "image_src string, downloaded long, save_location string, category string"
)
# Read in the Kinesis stream to a dataframe
df_pin = read_stream('streaming-129a67850695-pin', schema)
df_pin.limit(5).display()

# Store as a Global Temporary View for use by the cleaning notebook
df_pin.createOrReplaceGlobalTempView("gtv_129a67850695_stream_pin")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Clean the pin data

# COMMAND ----------

# MAGIC %run "./Clean Pin Data" $mode="Stream"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Save pin to a delta table

# COMMAND ----------

# Get the Global Temporary View from the cleaning notebook
df_pin_clean = spark.table("global_temp.gtv_129a67850695_stream_pin_clean")
df_pin_clean.limit(5).display()

# Write the cleaned dataframe to a Delta Table 
write_stream('129a67850695_pin_table', df_pin_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read the geo Kinesis stream

# COMMAND ----------

# Define the structure of the array in the "data" column
schema = ("ind long, timestamp string, latitude double, longitude double, country string")

# Read in the Kinesis stream to a dataframe
df_geo = read_stream('streaming-129a67850695-geo', schema)
df_geo.limit(5).display()

# Store as a Global Temporary View for use by the cleaning notebook
df_geo.createOrReplaceGlobalTempView("gtv_129a67850695_stream_geo")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Clean the geo data

# COMMAND ----------

# MAGIC %run "./Clean Geo Data" $mode="Stream"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Save geo to a delta table

# COMMAND ----------

# Get the Global Temporary View from the cleaning notebook
df_geo_clean = spark.table("global_temp.gtv_129a67850695_stream_geo_clean")
df_geo_clean.limit(5).display()

# Write the cleaned dataframe to a Delta Table 
write_stream('129a67850695_geo_table', df_geo_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read the user Kinesis stream

# COMMAND ----------

# Define the structure of the array in the "data" column
schema = ("ind long, first_name string, last_name string, age long, date_joined string")

# Read in the Kinesis stream to a dataframe
df_user = read_stream('streaming-129a67850695-user', schema)
df_user.limit(5).display()

# Store as a Global Temporary View for use by the cleaning notebook
df_user.createOrReplaceGlobalTempView("gtv_129a67850695_stream_user")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Clean the user data

# COMMAND ----------

# MAGIC %run "./Clean User Data" $mode="Stream"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Save user to a delta table

# COMMAND ----------

# Get the Global Temporary View from the cleaning notebook
df_user_clean = spark.table("global_temp.gtv_129a67850695_stream_user_clean")
df_user_clean.limit(5).display()

# Write the cleaned dataframe to a Delta Table 
write_stream('129a67850695_user_table', df_user_clean)
