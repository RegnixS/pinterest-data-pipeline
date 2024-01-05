# Databricks notebook source
# MAGIC %md
# MAGIC #Clean the User Data 
# MAGIC 1. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624721/command/40535148915789">Drop duplicates and empty rows.</a>
# MAGIC 2. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624721/command/40535148915791">Combine first and last names.</a>
# MAGIC 3. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624721/command/40535148915793">Drop first_name and last_name.</a>
# MAGIC 4. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624721/command/40535148915795">Convert date_joined to timestamp data type.</a>
# MAGIC 5. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624721/command/40535148915798">Reorder the dataframe.</a>
# MAGIC 6. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624721/command/40535148915802">Copy Cleaned Data to Global Temporary View</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ##Drop duplicates and empty rows

# COMMAND ----------

df_user = spark.table("global_temp.df_129a67850695_user")
df_user_clean = df_user.distinct()
df_user_clean = df_user_clean.dropna()

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
df_user_clean.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Copy cleaned dataframe to global temporary table

# COMMAND ----------

df_user_clean.createOrReplaceGlobalTempView("df_129a67850695_user_clean")
