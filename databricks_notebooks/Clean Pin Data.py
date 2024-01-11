# Databricks notebook source
# MAGIC %md
# MAGIC #Clean the Pin Data 
# MAGIC 1. Initialize and drop duplicates
# MAGIC 2. Replace entries with no relevant data in each column with nulls
# MAGIC 3. Perform the necessary transformations on the follower_count to ensure every entry is a number and the data type of this column is integer
# MAGIC 5. Clean the data in the save_location column to include only the save location path
# MAGIC 6. Rename the index column to ind
# MAGIC 7. Reorder the dataframe
# MAGIC 8. Copy cleaned data to Global Temporary View

# COMMAND ----------

# MAGIC %md
# MAGIC ##Initialize notebook and drop duplicates

# COMMAND ----------

# Define the input parameter with a default of "Batch" for batch processing
dbutils.widgets.dropdown("mode", "Batch", ["Batch"])
print("Running in " + dbutils.widgets.get("mode") + " mode.")

# If "Batch" or "Stream" mode, use appropriate temp view
if (dbutils.widgets.get("mode") == "Batch"):
    df_pin = spark.table("global_temp.gtv_129a67850695_pin")
elif(dbutils.widgets.get("mode") == "Stream"):
    df_pin = spark.table("global_temp.gtv_129a67850695_stream_pin")
else:
    raise Exception("Incorrect input for mode parameter")

# Drop duplicates
df_pin_clean = df_pin.distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Replace irrelevant column data with nulls

# COMMAND ----------

df_pin_clean = df_pin_clean.replace({'No description available Story format': None}, subset=['description'])
df_pin_clean = df_pin_clean.replace({'User Info Error': None}, subset=['follower_count'])
df_pin_clean = df_pin_clean.replace({'Image src error.': None}, subset=['image_src'])
df_pin_clean = df_pin_clean.replace({'User Info Error': None}, subset=['poster_name'])
df_pin_clean = df_pin_clean.replace({'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e': None}, subset=['tag_list'])
df_pin_clean = df_pin_clean.replace({'No Title Data Available': None}, subset=['title'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Convert follower_count to integer

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
df_pin_clean = df_pin_clean.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
df_pin_clean = df_pin_clean.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))
df_pin_clean = df_pin_clean.withColumn("follower_count", df_pin_clean["follower_count"].cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Clean data in save_location

# COMMAND ----------

df_pin_clean = df_pin_clean.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rename index to ind

# COMMAND ----------

df_pin_clean = df_pin_clean.withColumnRenamed("index", "ind")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reorder columns

# COMMAND ----------

df_pin_clean = df_pin_clean.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")
df_pin_clean.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Copy cleaned dataframe to global temporary table

# COMMAND ----------

# If "Batch" or "Stream" mode, use appropriate temp view
if (dbutils.widgets.get("mode") == "Batch"):
    df_pin_clean.createOrReplaceGlobalTempView("gtv_129a67850695_pin_clean")
elif(dbutils.widgets.get("mode") == "Stream"):
    df_pin_clean.createOrReplaceGlobalTempView("gtv_129a67850695_stream_pin_clean")
print("Global Temp View created for " + dbutils.widgets.get("mode") + " mode.")
