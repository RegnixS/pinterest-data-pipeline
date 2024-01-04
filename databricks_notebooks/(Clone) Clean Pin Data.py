# Databricks notebook source
# MAGIC %md
# MAGIC #Clean the Pin Data 
# MAGIC 1. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/4155037068217984/command/4155037068218046">Drop duplicates and empty rows</a>
# MAGIC 2. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/4155037068217984/command/4155037068218047">Replace entries with no relevant data in each column with Nulls</a>
# MAGIC 3. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/4155037068217984/command/4155037068218053">Perform the necessary transformations on the follower_count to ensure every entry is a number and the data type of this column is integer</a>
# MAGIC 5. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/4155037068217984/command/4155037068218426">Clean the data in the save_location column to include only the save location path</a>
# MAGIC 6. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/4155037068217984/command/4155037068218427">Rename the index column to ind</a>
# MAGIC 7. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/4155037068217984/command/4155037068218431">Reorder the dataframe</a>
# MAGIC 8. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/4155037068217984/command/4155037068218429">Copy Cleaned Data to Global Temporary View</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ##Drop duplicates and empty rows

# COMMAND ----------

df_pin = spark.table("global_temp.df_pin")
df_pin_clean = df_pin.distinct()
df_pin_clean = df_pin_clean.dropna()

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
df_pin_clean.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Copy cleaned dataframe to global temporary table

# COMMAND ----------

df_pin_clean.createOrReplaceGlobalTempView("df_pin_clean")
