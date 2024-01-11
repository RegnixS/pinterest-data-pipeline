# Databricks notebook source
# MAGIC %md
# MAGIC #Data Analysis
# MAGIC 1. Create Dataframes from Global Temporary Views
# MAGIC 2. Most popular category in each country
# MAGIC 3. Most popular category each year
# MAGIC 4. Most followers in each country
# MAGIC 5. Most popular category for different age groups
# MAGIC 6. Median follower count for different age groups
# MAGIC 7. How many users have joined each year
# MAGIC 8. Median follow count of users based on their joining year
# MAGIC 9. Median follow count of users based on their joining year and age group

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dataframes from Global Temporary Views

# COMMAND ----------

df_pin = spark.table("global_temp.gtv_129a67850695_pin_clean")
df_geo = spark.table("global_temp.gtv_129a67850695_geo_clean")
df_user = spark.table("global_temp.gtv_129a67850695_user_clean")

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
# MAGIC Step 1: For each country find the user with the most followers

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import desc, rank
# Create a window that orders by descending "follower_count" for each "country"
window_spec = Window.partitionBy("country").orderBy(desc("follower_count"))
# Join df_pin and df_geo together using common column "ind"
# Rank "follower_count" using the window
# Only show the highest rank
# Select the columns for the output
# Drop duplicates as there can be multiple pins by the same poster
df_most_followers_by_country = df_pin.join(df_geo, df_pin["ind"] == df_geo["ind"]) \
    .withColumn("rank", rank().over(window_spec)) \
    .filter("rank = 1") \
    .select("country", "poster_name", "follower_count") \
    .dropDuplicates(["poster_name", "country"])
display(df_most_followers_by_country)

# COMMAND ----------

# MAGIC %md
# MAGIC ### The user with the most followers in all countries
# MAGIC Step 2: Based on the above query, find the country with the user with most followers.

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
# MAGIC In case only the top most category is needed per age group

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
df_median_follower_counts_by_age_group = df_pin.join(
        df_user_with_age_groups.dropDuplicates(["user_name","age"]), 
        df_pin["ind"] == df_user_with_age_groups["ind"]
    ) \
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
df_median_follower_counts_by_joining_year = df_pin.join(
        df_user.dropDuplicates(["user_name","age"]),
        df_pin["ind"] == df_user["ind"]
    ) \
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
df_median_follower_counts_by_joining_year_and_age_group = df_pin.join(
        df_user_with_age_groups.dropDuplicates(["user_name","age"]),
        df_pin["ind"] == df_user_with_age_groups["ind"]
    ) \
    .withColumn("post_year", year("date_joined")) \
    .groupby("age_group", "post_year").agg(percentile_approx("follower_count", 0.5).alias("median_follower_count")) \
    .orderBy("age_group", "post_year") \
    .filter("post_year >= 2015 and post_year <= 2020") \
    .display()
