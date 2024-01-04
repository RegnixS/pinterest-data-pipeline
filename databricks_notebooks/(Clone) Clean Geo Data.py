# Databricks notebook source
# MAGIC %md
# MAGIC #Clean the Geo Data 
# MAGIC 1. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624684/command/3974944181624686">Drop duplicates and empty rows</a>
# MAGIC 2. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624684/command/3974944181624688">Create new coordinates column based on latitiude and longtitude</a>
# MAGIC 3. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624684/command/3974944181624690">Drop latitude and longtitude</a>
# MAGIC 5. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624684/command/3974944181624692">Convert timestamp to timestamp data type</a>
# MAGIC 6. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624684/command/3974944181624696">Reorder the dataframe</a>
# MAGIC 7. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624684/command/3974944181624698">Copy Cleaned Data to Global Temporary View</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ##Drop duplicates and empty rows

# COMMAND ----------

df_geo = spark.table("global_temp.df_geo")
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
df_geo_clean.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Copy cleaned dataframe to global temporary table

# COMMAND ----------

df_geo_clean.createOrReplaceGlobalTempView("df_geo_clean")

# COMMAND ----------


