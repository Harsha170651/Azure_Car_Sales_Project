# Databricks notebook source
# MAGIC %md
# MAGIC # DATA **READING**

# COMMAND ----------

df = spark.read.format("parquet")\
    .option('inferschema','true')\
    .option('header','true')\
    .load("abfss://bronze@sales06dl.dfs.core.windows.net/raw data")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = df.withColumn("Model_category",split(col("Model_ID"),"-")[0])

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn("units_sold",col("units_sold").cast(StringType())).display()

# COMMAND ----------

df = df.withColumn('revperunit',col('Revenue')/col('Units_Sold'))
df.display()

# COMMAND ----------

display(df.groupBy('year','BranchName').agg(sum('units_sold').alias('total_units')).sort('year','total_units',ascending=[1,0]))

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA WRITING

# COMMAND ----------

df.write.format('parquet')\
    .mode('append')\
    .option('path', 'abfss://silver@sales06dl.dfs.core.windows.net/carsales')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quering

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@sales06dl.dfs.core.windows.net/carsales`

# COMMAND ----------

