# Databricks notebook source
# snowflake connection options
sfOptions = {
  "sfURL": "https://whpxseb-ke63785.snowflakecomputing.com",
  "sfUser": "<username>",
  "sfPassword": "<passoword>",
  "sfDatabase": "EV_POP_DATA",
  "sfSchema": "DBO",
  "sfWarehouse": "MYWH"
}

# COMMAND ----------

# Write the data from the Delta DataFrame to a new Snowflake table using the connector library
dfData = spark.sql("select * from ev_pop_data")
dfData.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "data_tbl_from_db") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

dfApprovals = spark.sql("select * from ev_pop_approvals")
dfApprovals.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "approvals_tbl_from_db") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

dfView = spark.sql("select * from ev_pop_view")
dfApprovals.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "view_tbl_from_db") \
  .mode("overwrite") \
  .save()
