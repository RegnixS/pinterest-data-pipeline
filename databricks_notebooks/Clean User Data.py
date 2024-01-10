# Databricks notebook source
# MAGIC %md
# MAGIC #Clean the User Data 
# MAGIC 1. Initialize and drop duplicates
# MAGIC 2. Combine first and last names
# MAGIC 3. Drop first_name and last_name
# MAGIC 4. Convert date_joined to timestamp data type
# MAGIC 5. Reorder the dataframe
# MAGIC 6. Copy Cleaned Data to Global Temporary View

# COMMAND ----------

# MAGIC %md
# MAGIC ##Initialize notebook and drop duplicates

# COMMAND ----------

# Define the input parameter with a default of "Batch" for batch processing
dbutils.widgets.dropdown("mode", "Batch", ["Batch"])
print("Running in " + dbutils.widgets.get("mode") + " mode.")

# If "Batch" or "Stream" mode, use appropriate temp view
if (dbutils.widgets.get("mode") == "Batch"):
    df_user = spark.table("global_temp.gtv_129a67850695_user")
elif(dbutils.widgets.get("mode") == "Stream"):
    df_pin = spark.table("global_temp.gtv_129a67850695_stream_user")
else:
    raise Exception("Incorrect input for mode parameter")

# Drop duplicates
df_user_clean = df_user.distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Combine first and last names

# COMMAND ----------

from pyspark.sql.functions import concat, lit
df_user_clean = df_user_clean.withColumn("user_name", concat("first_name", lit(" "), "last_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Drop columns first_name and last_name

# COMMAND ----------

df_user_clean = df_user_clean.drop("first_name")
df_user_clean = df_user_clean.drop("last_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Convert date_joined to timestamp data type

# COMMAND ----------

from pyspark.sql.functions import to_timestamp
df_user_clean = df_user_clean.withColumn("date_joined", to_timestamp("date_joined"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reorder columns

# COMMAND ----------

df_user_clean = df_user_clean.select("ind", "age", "user_name", "date_joined")
df_user_clean.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Copy cleaned dataframe to global temporary table

# COMMAND ----------

# If "Batch" or "Stream" mode, use appropriate temp view
if (dbutils.widgets.get("mode") == "Batch"):
    df_user_clean.createOrReplaceGlobalTempView("gtv_129a67850695_user_clean")
elif(dbutils.widgets.get("mode") == "Stream"):
    df_user_clean.createOrReplaceGlobalTempView("gtv_129a67850695_stream_user_clean")
print("Global Temp View created for " + dbutils.widgets.get("mode") + " mode.")
