# Databricks notebook source
# MAGIC %md
# MAGIC #Kinesis Streaming
# MAGIC 1. Get the AWS authentication key file
# MAGIC 2. Define read_stream function
# MAGIC 3. Define write_stream function
# MAGIC 4. Read the pin Kinesis stream
# MAGIC 5. Clean the pin data
# MAGIC 6. Save pin to a delta table
# MAGIC 7. Read the geo Kinesis stream
# MAGIC 8. Clean the geo data
# MAGIC 9. Save geo to a delta table
# MAGIC 10. Read the user Kinesis stream
# MAGIC 11. Clean the user data
# MAGIC 12. Save user to a delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get authentication File

# COMMAND ----------

# MAGIC %run "/Repos/rgducke@gmail.com/pinterest-data-pipeline/databricks_notebooks/Get Authentication Keys"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Define read_stream function

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
# MAGIC ##Define write_stream function

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

# MAGIC %run "/Repos/rgducke@gmail.com/pinterest-data-pipeline/databricks_notebooks/Clean Pin Data" $mode="Stream"

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

# MAGIC %run "/Repos/rgducke@gmail.com/pinterest-data-pipeline/databricks_notebooks/Clean Geo Data" $mode="Stream"

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

# MAGIC %run "/Repos/rgducke@gmail.com/pinterest-data-pipeline/databricks_notebooks/Clean User Data" $mode="Stream"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Save user to a delta table

# COMMAND ----------

# Get the Global Temporary View from the cleaning notebook
df_user_clean = spark.table("global_temp.gtv_129a67850695_stream_user_clean")
df_user_clean.limit(5).display()

# Write the cleaned dataframe to a Delta Table 
write_stream('129a67850695_user_table', df_user_clean)
