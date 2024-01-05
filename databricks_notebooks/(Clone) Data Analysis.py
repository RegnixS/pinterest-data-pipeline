# Databricks notebook source
# MAGIC %md
# MAGIC #Data Analysis
# MAGIC 1. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624748/command/198588058359915">Create Dataframes from Global Temporary Views</a>
# MAGIC 2. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624748/command/3974944181624754">Most popular category in each country</a>
# MAGIC 3. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624748/command/1019958287663025">Most popular category each year</a>
# MAGIC 4. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624748/command/1071625537364147">Most followers in each country</a>
# MAGIC 5. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624748/command/2477640128388017">Most popular category for different age groups</a>
# MAGIC 6. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624748/command/198588058359651">Median follower count for different age groups</a>
# MAGIC 7. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624748/command/198588058359653">How many users have joined each year</a>
# MAGIC 8. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624748/command/198588058360003">Median follow count of users based on their joining year</a>
# MAGIC 9. <a href="https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/3974944181624748/command/198588058360026">Median follow count of users based on their joining year and age group</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dataframes from Global Temporary Views

# COMMAND ----------

df_pin = spark.table("global_temp.df_pin_clean")
df_geo = spark.table("global_temp.df_geo_clean")
df_user = spark.table("global_temp.df_user_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Most popular category in each country

# COMMAND ----------

# Join df_pin and df_geo together using common column "ind"
# Count number of pins for each "country" + "category" combination
# Rename the count column
# Order by "country", then descending "category_count", then "category"
df_pop_category_by_country = df_pin.join(df_geo, df_pin["ind"] == df_geo["ind"]) \
    .groupBy("country", "category") \
    .count() \
    .withColumnRenamed("count", "category_count") \
    .orderBy("country", "category_count", "category", ascending=[1,0,1]) \
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Most popular category each year (between 2018 and 2022)

# COMMAND ----------

from pyspark.sql.functions import year
# Join df_pin and df_geo together using common column "ind"
# Use year() function to extract just the year part of the timestamp to "post_year"
# Count number of pins for each "post_year" + "category" combination
# Rename the count column
# Order by descending "post_year", then descending "category_count", then "category"
# Filter between 2018 and 2022
df_pop_category_by_year = df_pin.join(df_geo, df_pin["ind"] == df_geo["ind"]) \
    .withColumn("post_year", year("timestamp")) \
    .groupBy("post_year", "category") \
    .count() \
    .withColumnRenamed("count", "category_count") \
    .orderBy("post_year", "category_count", "category", ascending=[0,0,1]) \
    .filter("post_year >= 2018 and post_year <= 2022") \
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Users with the most followers in each country

# COMMAND ----------

# Join df_pin and df_geo together using common column "ind"
# Select the columns for the output
# Order by "country", then descending "follower_count", then "poster_name"
df_most_followers_by_country = df_pin.join(df_geo, df_pin["ind"] == df_geo["ind"]) \
    .select("country", "poster_name", "follower_count") \
    .orderBy("country", "follower_count", "poster_name", ascending=[1,0,1])
display(df_most_followers_by_country)
# Note: This dataframe is based on pins with the highest follower counts, so will show duplicate users.

# COMMAND ----------

# MAGIC %md
# MAGIC ### The user with the most followers in all countries

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import desc, rank
# Create a window that orders by descending "follower_count" over the whole dataframe
window_spec = Window.partitionBy().orderBy(desc("follower_count"))
# Rank "follower_count" using the window
# Only show the highest rank
# Select the columns for the output
df_most_followers_overall = df_most_followers_by_country.withColumn("rank", rank().over(window_spec)) \
    .filter("rank = 1") \
    .select("country", "poster_name", "follower_count") \
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Most popular categories for different age groups

# COMMAND ----------

from pyspark.sql.functions import when
# Use the conditional when clause to bin ages into age groups
df_user_with_age_groups = df_user.withColumn("age_group", when(df_user["age"].between(18,24), "18-24") \
   .when(df_user["age"].between(25,35), "25-35") \
   .when(df_user["age"].between(36,50), "36-50") \
   .otherwise("50+")
)
# Join df_pin and df_user_with_age_groups together using common column "ind"
# Count number of pins for each "age_group" + "category" combination
# Rename the count column
# Order by "age_group", then descending "category_count", then "category"
df_pop_category_by_age_groups = df_pin.join(df_user_with_age_groups, df_pin["ind"] == df_user_with_age_groups["ind"]) \
    .groupBy("age_group", "category") \
    .count() \
    .withColumnRenamed("count", "category_count") \
    .orderBy("age_group", "category_count", "category", ascending=[1,0,1])
display(df_pop_category_by_age_groups)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Only the top most popular category for each different age group (including ties)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import desc, rank
# Create a window that orders by descending "category_count" within each "age_group"
window_spec = Window.partitionBy("age_group").orderBy(desc("category_count"))
# Rank "category_count" using the window
# Only show the highest rank
# Select the columns for the output
df_top_category_by_age_groups = df_pop_category_by_age_groups.withColumn("rank", rank().over(window_spec)) \
    .filter("rank = 1") \
    .select("age_group", "category", "category_count") \
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Median follower count for different age groups

# COMMAND ----------

from pyspark.sql.functions import percentile_approx
# df_user_with_age_groups contains duplicate users because it's based on pin data. Drop the duplicates first so median is calculated correctly
# Join df_pin and df_user_with_age_groups together using common column "ind"
# Find median values of "follower_count" for each "age_group"
# median() function has been deprecated, so we use percentile_approx() instead where 0.5 is the halfway point like median
# Order by "age_group"
df_median_follower_counts_by_age_group = df_pin.join(df_user_with_age_groups.dropDuplicates(["user_name","age"]), \
    df_pin["ind"] == df_user_with_age_groups["ind"]) \
    .groupby("age_group").agg(percentile_approx("follower_count", 0.5).alias("median_follower_count")) \
    .orderBy("age_group") \
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##How many users join each year

# COMMAND ----------

from pyspark.sql.functions import year
# Drop duplicate users from df_user
# Use year() function to extract just the year part of the timestamp to "post_year"
# Count number of distinct users for each "post_year"
# Rename the count column
# Filter between 2015 and 2020
df_date_joined_by_year = df_user.dropDuplicates(["user_name","age"]) \
    .withColumn("post_year", year("date_joined")) \
    .groupBy("post_year") \
    .count() \
    .withColumnRenamed("count", "number_users_joined") \
    .filter("post_year >= 2015 and post_year <= 2020") \
    .display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##Median follower count of users based on joining year

# COMMAND ----------

from pyspark.sql.functions import percentile_approx
# df_user contains duplicate users because it's based on pin data. Drop the duplicates first so median is calculated correctly
# Join df_pin and df_user together using common column "ind"
# Find median values of "follower_count" for each "post_year"
# median() function has been deprecated, so we use percentile_approx() instead where 0.5 is the halfway point like median
# Order by "post_year"
# Filter between 2015 and 2020
df_median_follower_counts_by_joining_year = df_pin.join(df_user.dropDuplicates(["user_name","age"]), \
    df_pin["ind"] == df_user["ind"]) \
    .withColumn("post_year", year("date_joined")) \
    .groupby("post_year").agg(percentile_approx("follower_count", 0.5).alias("median_follower_count")) \
    .orderBy("post_year") \
    .filter("post_year >= 2015 and post_year <= 2020") \
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Median follower count of users based on joining year and age group

# COMMAND ----------

from pyspark.sql.functions import percentile_approx
# df_user_with_age_groups contains duplicate users because it's based on pin data. Drop the duplicates first so median is calculated correctly
# Join df_pin and df_user_with_age_groups together using common column "ind"
# Find median values of "follower_count" for each "age_group" and "post_year" combination
# median() function has been deprecated, so we use percentile_approx() instead where 0.5 is the halfway point like median
# Order by "age_group" and "post_year"
# Filter between 2015 and 2020
df_median_follower_counts_by_joining_year_and_age_group = df_pin.join(df_user_with_age_groups.dropDuplicates(["user_name","age"]), \
    df_pin["ind"] == df_user_with_age_groups["ind"]) \
    .withColumn("post_year", year("date_joined")) \
    .groupby("age_group", "post_year").agg(percentile_approx("follower_count", 0.5).alias("median_follower_count")) \
    .orderBy("age_group", "post_year") \
    .filter("post_year >= 2015 and post_year <= 2020") \
    .display()
