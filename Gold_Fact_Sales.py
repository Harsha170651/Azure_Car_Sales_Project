# Databricks notebook source
# MAGIC %md
# MAGIC # CRATE FACT TABLE
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading Silver Data**

# COMMAND ----------


df_silver = spark.sql("select * from parquet.`abfss://silver@sales06dl.dfs.core.windows.net/carsales`")

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading all dimensions**

# COMMAND ----------

df_dealer = spark.sql("select * from cars_catalog.gold.dim_dealer")

df_branch = spark.sql("select * from cars_catalog.gold.dim_branch")

df_date = spark.sql("select * from cars_catalog.gold.dim_date")

df_model = spark.sql("select * from cars_catalog.gold.dim_model")

# COMMAND ----------

# MAGIC %md
# MAGIC **Bringing keys to FACT table**

# COMMAND ----------

df_fact = df_silver.join(df_branch, df_silver.Branch_ID == df_branch.Branch_ID, "left")\
                   .join(df_date, df_silver.Date_ID == df_date.Date_ID, "left")\
                   .join(df_dealer, df_silver.Dealer_ID == df_dealer.Dealer_ID, "left")\
                   .join(df_model, df_silver.Model_ID == df_model.MODEL_ID, "left")\
                   .select(df_silver["Revenue"],df_silver["units_sold"],df_silver["revperunit"],df_branch["dim_branch_key"],df_date["dim_date_key"],df_dealer["dim_dealer_key"],df_model["dim_model_key"])

# COMMAND ----------

df_fact.display()

# COMMAND ----------

