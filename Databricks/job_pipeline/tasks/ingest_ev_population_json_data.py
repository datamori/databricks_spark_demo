# Databricks notebook source
# DBTITLE 0,Imports & Constants
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

import time

RAW_DATA_PATH = "/mnt/mydatalake2/ElectricVehiclePopulationData.json"

# COMMAND ----------

#EV_population_json_data parser
class evPopJsonParser:
    def __init__(self, src_path):
        self._src_path = src_path
        self._dfRaw = None
        self._dfView = None
        self._dfViewFlattened = None
        self._dfApprovals = None
        self._dfApprovalsFlattened = None
        self._dfData = None
        epoch = time.time()
        self.RAW_DATA_ID = "%d" % (epoch)
        self._loadRawData()
        
    def _loadRawData(self):
        #load the raw josn data
        self._dfRaw = spark.read.option("multiline","true").json(self._src_path)\
                .withColumn("raw_data_id", F.lit(self.RAW_DATA_ID))
        # check for any _corrupt_record
        if "_corrupt_record" in self._dfRaw.columns:
            # filter out any rows with null values
            self._dfRaw = self._dfRaw.filter(col("_corrupt_record").isNull())
            # drop the _corrupt_record column
            self._dfRaw = self._dfRaw.drop("_corrupt_record")

    def getViewFlattened(self):
        if self._dfViewFlattened != None:
            return self._dfViewFlattened
        
        #get approval list from meta/view
        self._dfView = self._dfRaw.select(F.col("meta.view").alias("view"))#.selectExpr("view as view")
    
        #Get the field names of "view" dynamically without specifying them statically.
        #And add raw data id as for the association key
        self._dfViewFlattened = self._dfView.select([F.col("view." + field.name).alias(field.name) for field in self._dfView.schema["view"].dataType.fields])\
                                       .withColumn("raw_data_id", F.lit(self.RAW_DATA_ID))
    
        return self._dfViewFlattened
    
    def getApprovalsFalttened(self):
        if self._dfApprovalsFlattened != None:
            return _dfApprovalsFlattened
    
        #get approvals data from the flattend view df. 
        self._dfApprovals = self._dfViewFlattened.select(F.col("approvals").alias("approvals"))\
                                    .selectExpr("explode(approvals) as approval")

        #Get the field names of "approvals" in a general way without specifying them statically.
        #And add raw data id as for the association key
        self._dfApprovalsFlattened = self._dfApprovals\
                                             .select([F.col("approval." + field.name).alias(field.name) for field in self._dfApprovals.schema["approval"].dataType.fields])\
                                             .withColumn("raw_data_id", F.lit(self.RAW_DATA_ID))
    
        return self._dfApprovalsFlattened
    
    def _getColumnListFromView(self):
        dfColumns = self._dfViewFlattened.select(F.col("columns").alias("columns")).selectExpr("explode(columns) as column")
        dfColumnNames = dfColumns.select(F.col("column.name").alias("name"))
        
        return list(dfColumnNames.select('name').toPandas()['name'])
    
    def extractDataObjAndCreateFlattenedTable(self):
        if self._dfData != None:
            return self._dfData
        #parse data object from the root level json, which is array of arrays, build flattend table
        #explode array of arrays to array column first
        self._dfData = self._dfRaw.selectExpr("explode(data) as data")

        #now expend the array column to per-element columns
        #get the lengh of the data array
        nCols = len(self._dfData.select("data").take(1)[0]["data"])
        self._dfData = self._dfData.select("*", *[F.col("data")[i].alias("col"+str(i+1)) for i in range(nCols)]).drop("data")

        #replace the column header with the extracted column list and add RAW_DATA_ID key value
        lstColumns = self._getColumnListFromView()
        self._dfData = self._dfData.toDF(*lstColumns).withColumn("raw_data_id", F.lit(self.RAW_DATA_ID))

        return self._dfData

# COMMAND ----------

theParser = evPopJsonParser(RAW_DATA_PATH)
#theParser._dfRaw.show()

# COMMAND ----------

dfView = theParser.getViewFlattened()
display(dfView)
dfApprovals = theParser.getApprovalsFalttened()
display(dfApprovals)
dfData = theParser.extractDataObjAndCreateFlattenedTable()
display(dfData)

#dfData.createOrReplaceTempView("ev_pop_data_tmp")
#spark.catalog.dropTempView("ev_pop_data_tmp")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS ev_pop_view")
spark.sql("DROP TABLE IF EXISTS ev_pop_approvals")
spark.sql("DROP TABLE IF EXISTS ev_pop_data")

# COMMAND ----------

dfView.write.format("parquet").saveAsTable("ev_pop_view")
dfApprovals.write.format("parquet").saveAsTable("ev_pop_approvals")
dfData.write.format("parquet").saveAsTable("ev_pop_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ev_pop_data

# COMMAND ----------


