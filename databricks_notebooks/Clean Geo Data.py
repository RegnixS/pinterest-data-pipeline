# Databricks notebook source
# MAGIC %md
# MAGIC #Clean the Geo Data 
# MAGIC 1. Initialize and drop duplicates
# MAGIC 2. Create new coordinates column based on latitiude and longtitude
# MAGIC 3. Drop latitude and longtitude
# MAGIC 5. Convert timestamp to timestamp data type
# MAGIC 6. Reorder the dataframe
# MAGIC 7. Copy Cleaned Data to Global Temporary View

# COMMAND ----------

# MAGIC %md
# MAGIC ##Initialize notebook and drop duplicates

# COMMAND ----------

# Define the input parameter with a default of "Batch" for batch processing
dbutils.widgets.dropdown("mode", "Batch", ["Batch"])
print("Running in " + dbutils.widgets.get("mode") + " mode.")

# If "Batch" or "Stream" mode, use appropriate temp view
if(dbutils.widgets.get("mode") == "Batch"):
    df_geo = spark.table("global_temp.gtv_129a67850695_geo")
elif(dbutils.widgets.get("mode") == "Stream"):
    df_pin = spark.table("global_temp.gtv_129a67850695_stream_geo")
else:
    raise Exception("Incorrect input for mode parameter")

# Drop duplicates
df_geo_clean = df_geo.distinct()
df_geo_clean = df_geo_clean.dropna()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create array column coordinates using latitude and longtitude

# COMMAND ----------

from pyspark.sql.functions import array
df_geo_clean = df_geo_clean.withColumn("coordinates", array("latitude", "longitude"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Drop columns latitude and longitude

# COMMAND ----------

df_geo_clean = df_geo_clean.drop("latitude")
df_geo_clean = df_geo_clean.drop("longitude")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Convert timestamp to timestamp data type

# COMMAND ----------

from pyspark.sql.functions import to_timestamp
df_geo_clean = df_geo_clean.withColumn("timestamp", to_timestamp("timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reorder columns

# COMMAND ----------

df_geo_clean = df_geo_clean.select("ind", "country", "coordinates", "timestamp")
df_geo_clean.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Copy cleaned dataframe to global temporary table

# COMMAND ----------

# If "Batch" or "Stream" mode, use appropriate temp view
if (dbutils.widgets.get("mode") == "Batch"):
    df_geo_clean.createOrReplaceGlobalTempView("gtv_129a67850695_geo_clean")
elif(dbutils.widgets.get("mode") == "Stream"):
    df_geo_clean.createOrReplaceGlobalTempView("gtv_129a67850695_stream_geo_clean")
print("Global Temp View created for " + dbutils.widgets.get("mode") + " mode.")
