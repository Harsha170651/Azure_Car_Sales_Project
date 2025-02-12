# Databricks notebook source
from pyspark.sql.functions import col, lit
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATEING  FLAG PARAMETER

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')
print(incremental_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATING DIMENSIONAL MODEL

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating source for the Dimensional model

# COMMAND ----------

df_src = spark.sql('''
    select distinct(Branch_ID) AS Branch_ID, BranchName 
    from parquet.`abfss://silver@sales06dl.dfs.core.windows.net/carsales`
'''
)

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating sink for the dimensional model(Just Bring the schema)

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_Branch'):
    df_sink = spark.sql('''
    select dim_Branch_key,Branch_ID,BranchName
    from cars_catalog.gold.dim_Branch
    '''
)
else:
    df_sink = spark.sql('''
    select 1 as dim_Branch_key,Branch_ID,BranchName
    from parquet.`abfss://silver@sales06dl.dfs.core.windows.net/carsales`
    where 1=0
    '''    )


# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Joining source and sink to extract the data

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['Branch_ID'] == df_sink['Branch_ID'], 'left').select(df_src['Branch_ID'],df_src['BranchName'],df_sink['dim_Branch_key'])

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## df_filter_old_data

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_branch_key').isNotNull())
df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # df_filter_new_data

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_branch_key').isNull()).select(df_src['Branch_ID'],df_src['BranchName'])
df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC **Fetch max surrogate key from existing Table**

# COMMAND ----------

if (incremental_flag == '0'):
    max_value = 1
else:
    max_value_df = spark.sql("select max(dim_branch_key) from cars_catalog.gold.dim_branch")
    max_value = max_value_df.collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC **create surrogate key column and add the max surrogate key**

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_branch_key',max_value + monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Final_DF - df_filter_old + df_filter_new

# COMMAND ----------

df_final = df_filter_old.union(df_filter_new)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD TYPE - 1(UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# Incremental Load
if spark.catalog.tableExists("cars_catalog.gold.dim_branch"):
    delta_tbl = DeltaTable.forName(spark, "cars_catalog.gold.dim_branch")

    delta_tbl.alias("trg").merge(
        df_final.alias("src"), "trg.dim_Branch_key = src.dim_Branch_key"
    ).whenMatchedUpdateAll()\
     .whenNotMatchedInsertAll()\
     .execute()

# Initial Run
else:
    df_final.write.format("delta")\
        .mode("overwrite")\
        .option("path", "abfss://gold@sales06dl.dfs.core.windows.net/dim_branch")\
        .saveAsTable("cars_catalog.gold.dim_branch")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_branch