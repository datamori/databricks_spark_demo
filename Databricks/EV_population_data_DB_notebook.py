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
print(dfRaw.schema["meta"])

# COMMAND ----------

# DBTITLE 1,Flatten Approvals Data
#get approval list from meta/view/approval
dfApprovals = dfRaw.select(F.col("meta.view.approvals").alias("approvals")).selectExpr("explode(approvals) as approval")
#display(dfApprovals)

dfApprovalsFlattened = dfApprovals.select([F.col("approval." + field.name).alias(field.name) for field in dfApprovals.schema["approval"].dataType.fields])
display(dfApprovalsFlattened)


# COMMAND ----------

#get column list from the meta json
dfColumns = dfRaw.select(F.col("meta.view.columns").alias("columns")).selectExpr("explode(columns) as column")
dfColumnNames = dfColumns.select(F.col("column.name").alias("name"))
display(dfColumnNames)
lstColumns = list(dfColumnNames.select('name').toPandas()['name'])
print(lstColumns)


# COMMAND ----------

#structured normalization on view sub doucuments under meta
json_meta_view_schema = StructType("meta", StructType[
    StructField("view", StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("assetType", StringType()),
        StructField("attribution", StringType()),
        StructField("averageRating", StringType()),
        StructField("category", StringType()),
        StructField("createdAt", StringType()),
        StructField("description", StringType()),
        StructField("displayType", StringType()),
        StructField("downloadCount", StringType()),
        StructField("hideFromCatalog", BooleanType()),
        StructField("hideFromDataJson", BooleanType()),
        StructField("newBackend", BooleanType()),
        StructField("numberOfComments", IntegerType()),
        StructField("oid", IntegerType()),
        StructField("provenance", StringType()),
        StructField("publicationAppendEnabled", BooleanType()),
        StructField("publicationDate", LongType()),
        StructField("publicationGroup", LongType()),
        StructField("publicationStage", StringType()),
        StructField("rowsUpdatedAt", LongType()),
        StructField("rowsUpdatedBy", StringType()),
        StructField("tableId", IntegerType()),
        StructField("totalTimesRated", IntegerType()),
        StructField("viewCount", IntegerType()),
        StructField("viewLastModified", LongType()),
        StructField("viewType", StringType()),
        #StructField("approvals", StructType()),
        #StructField("columns", StructType())
    ]))
])


# Parse the "view" JSON data
dfMetaView = dfRaw.select("meta", F.from_json(F.col("view"), json_meta_view_schema))
dfMetaView.show()


# COMMAND ----------

#build data table from data object from the json, which is array of arrays
#explode array of arrays to array column first
dfData = dfRaw.selectExpr("explode(data) as data")
#.select("*").withColumn("id", F.monotonically_increasing_id())

#now expend the array column to per-element columns
#get the lent of the data array
nCols = len(dfData.select("data").take(1)[0]["data"])
dfData = dfData.select("*", *[F.col("data")[i].alias("col"+str(i+1)) for i in range(nCols)]).drop("data")
dfData = dfData.toDF(*lstColumns).withColumn("raw_data_id", F.lit(RAW_DATA_ID))

display(dfData)

