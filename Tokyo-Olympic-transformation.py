# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "7c143ade-5d59-4135-af72-99f77ce806bc",
"fs.azure.account.oauth2.client.secret": 'BhJ8Q~GircxY50JqfSvNu8r8faThoJRRZSN4~di4',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/91f3258c-186e-4520-9191-69dd7fd5d046/oauth2/token"}

dbutils.fs.mount(
source = "abfss://tokyoolympic@tokyoolympicproj.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymic"

# COMMAND ----------

Athletes = spark.read.format("csv").option("header","True").option("inferSchema","True").load("/mnt/tokyoolymic/raw data/Athletes.csv")
Coaches = spark.read.format("csv").option("header","True").option("inferSchema","True").load("/mnt/tokyoolymic/raw data/Coaches.csv")
EntriesGender = spark.read.format("csv").option("header","True").option("inferSchema","True").load("/mnt/tokyoolymic/raw data/EntriesGender.csv")
Medals = spark.read.format("csv").option("header","True").option("inferSchema","True").load("/mnt/tokyoolymic/raw data/Medal.csv")
Teams = spark.read.format("csv").option("header","True").option("inferSchema","True").load("/mnt/tokyoolymic/raw data/Teams.csv")

# COMMAND ----------

Athletes.printSchema()

# COMMAND ----------

EntriesGender.show()

# COMMAND ----------

EntriesGender.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

EntriesGender = EntriesGender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
        .withColumn("Total",col("Total").cast(IntegerType()))


# COMMAND ----------

Medals.show()

# COMMAND ----------

Medals.printSchema()

# COMMAND ----------

# Find the top countries with the highest number of gold medals
top_gold_medal_countries = Medals.orderBy("Gold", ascending=False).select("Team/NOC","Gold").show()

# COMMAND ----------

Athletes.write.option("Header","True").csv("/mnt/tokyoolymic/transformed data/Athletes.csv")
EntriesGender.write.option("Header","True").csv("/mnt/tokyoolymic/transformed data/EntriesGender.csv")
Coaches.write.option("Header","True").csv("/mnt/tokyoolymic/transformed data/Coaches.csv")
Medals.write.option("Header","True").csv("/mnt/tokyoolymic/transformed data/Medals.csv")
Teams.write.option("Header","True").csv("/mnt/tokyoolymic/transformed data/Teams.csv")

# COMMAND ----------


