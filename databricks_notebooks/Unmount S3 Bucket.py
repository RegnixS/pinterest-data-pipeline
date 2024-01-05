# Databricks notebook source
# MAGIC %md
# MAGIC #Unmount The S3 Bucket

# COMMAND ----------

dbutils.fs.unmount("/mnt/user-129a67850695-bucket")
