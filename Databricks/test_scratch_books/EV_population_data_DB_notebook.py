# Databricks notebook source
# DBTITLE 1,Imports & Constants
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

#generate unique_id to use as primary key
import time
epoch = time.time()
RAW_DATA_ID = "%d" % (epoch)

RAW_DATA_PATH = "/mnt/mydata/ElectricVehiclePopulationData.json"

# COMMAND ----------

# DBTITLE 1,Load Raw Data
#load the raw josn data and split
dfRaw = spark.read.option("multiline","true").json(RAW_DATA_PATH)\
.withColumn("raw_data_id", F.lit(RAW_DATA_ID))

# check for any _corrupt_record
if "_corrupt_record" in dfRaw.columns:
    # filter out any rows with null values
    dfRaw = dfRaw.filter(col("_corrupt_record").isNull())
    # drop the _corrupt_record column
    dfRaw = dfRaw.drop("_corrupt_record")
    
dfRaw.show()
#print(dfRaw.schema["meta"])

#save the dataframe to a temp view table
dfRaw.createOrReplaceTempView("ev_pop_data_raw")

# COMMAND ----------

# DBTITLE 1,Extract Meta/View object
#get approval list from meta/view/approval
dfView = dfRaw.select(F.col("meta.view").alias("view")).selectExpr("view as view")
display(dfView)

#Get the field names of "view" dynamically without specifying them statically.
#And add raw data id as for the association key
dfViewFlattend = dfView.select([F.col("view." + field.name).alias(field.name) for field in dfView.schema["view"].dataType.fields])\
.withColumn("raw_data_id", F.lit(RAW_DATA_ID))

display(dfViewFlattend)

#save the dataframe to a temp view table
dfViewFlattend.createOrReplaceTempView("ev_pop_data_view_flat")

# COMMAND ----------

# DBTITLE 1,Extract and Flatten Approvals Data
#get approvals data from the flattend view df. 
dfApprovals = dfViewFlattend.select(F.col("approvals").alias("approvals"))\
              .selectExpr("explode(approvals) as approval")

#get approvals data from meta/view/approval - just for json crawl excercise :-)
#dfApprovals = dfRaw.select(F.col("meta.view.approvals").alias("approvals")).selectExpr("explode(approvals) as approval")

display(dfApprovals)

#Get the field names of "approvals" in a general way without specifying them statically.
#And add raw data id as for the association key
dfApprovalsFlattened = dfApprovals.select([F.col("approval." + field.name).alias(field.name) for field in dfApprovals.schema["approval"].dataType.fields])\
.withColumn("raw_data_id", F.lit(RAW_DATA_ID))
display(dfApprovalsFlattened)

#save the dataframe to a temp view table
dfApprovalsFlattened.createOrReplaceTempView("ev_pop_data_approvals_flat")

# COMMAND ----------

# DBTITLE 1,Get Column Data and Convert to Python list object
#get column list from the meta json
#dfColumns = dfRaw.select(F.col("meta.view.columns").alias("columns")).selectExpr("explode(columns) as column")
dfColumns = dfViewFlattend.select(F.col("columns").alias("columns")).selectExpr("explode(columns) as column")
dfColumnNames = dfColumns.select(F.col("column.name").alias("name"))
#display(dfColumnNames)
lstColumns = list(dfColumnNames.select('name').toPandas()['name'])
print(lstColumns)


# COMMAND ----------

# DBTITLE 1,Create Data Table
#parse data object from the root level json, which is array of arrays, build flattend table
#explode array of arrays to array column first
dfData = dfRaw.selectExpr("explode(data) as data")
#.select("*").withColumn("id", F.monotonically_increasing_id())

#now expend the array column to per-element columns
#get the lent of the data array
nCols = len(dfData.select("data").take(1)[0]["data"])
dfData = dfData.select("*", *[F.col("data")[i].alias("col"+str(i+1)) for i in range(nCols)]).drop("data")

#replace the column header with the extracted column list and add RAW_DATA_ID key value
dfData = dfData.toDF(*lstColumns).withColumn("raw_data_id", F.lit(RAW_DATA_ID))

display(dfData)

#save the dataframe to a temp view table
dfData.createOrReplaceTempView("ev_pop_data_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ev_pop_data_table;

# COMMAND ----------


