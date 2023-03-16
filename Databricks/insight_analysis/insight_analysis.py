# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# COMMAND ----------

dfData = spark.sql("select * from ev_pop_data")
display(dfData)

# COMMAND ----------

# DBTITLE 1,Which one of the car make is more efficient?
#found string data type in Eletric Range. fix the prob first and drop rows have any null column value and the rows that have zero-value Electric_Rnage
dfEfficiency = dfData.select("Make", F.col("Electric Range").cast("int").alias("Electric_Range")).na.drop().filter("Electric_Range > 0")
dfEfficiency = dfEfficiency.groupBy("Make")\
  .agg(F.count("*").alias("count"),F.avg("Electric_Range").alias("Avg_Electric_Range"))\
  .orderBy("Avg_Electric_Range", ascending=False).limit(25)
dfEfficiency.show()

# COMMAND ----------

# DBTITLE 1,Is there any relationship between the choice of EV make and city?
dfMakeByLocations = dfData.groupBy("Make", "City")\
  .agg(F.count("*").alias("count"))\
  .orderBy("count", ascending=False)
dfMakeByLocations.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Which Plug-in Hybrid Electric Vehicle (PHEV) is preferred by buyers?
dfFavPhev = dfData.select("Make", "Model").filter(F.col("Electric Vehicle Type").contains("PHEV"))\
  .groupBy("Make", "Model")\
  .agg(F.count("*").alias("count"))\
  .orderBy("count", ascending=False).limit(25)
dfFavPhev.show()

# COMMAND ----------

# DBTITLE 1,Based on the data, which car make and model would you recommend?
#found string data type in Eletric Range. fix the prob first and drop rows have any null column value and the rows that have zero-value Electric_Rnage
dfBase = dfData.select("Model Year", "Make", "Model", F.col("Electric Range").cast("int").alias("Electric_Range")).na.drop().filter("Electric_Range > 0")

dfEfficiency = dfBase.groupBy("Make", "Model")\
  .agg(F.count("*").alias("count"),F.avg("Electric_Range").alias("Avg_Electric_Range"))\
  .orderBy("Avg_Electric_Range", ascending=False).limit(20)
dfEfficiency.show()

dfEfficiency2 = dfBase.groupBy("Model Year", "Make", "Model")\
  .agg(F.count("*").alias("count"),F.avg("Electric_Range").alias("Avg_Electric_Range"))\
  .orderBy("Avg_Electric_Range", ascending=False).limit(20)
dfEfficiency2.show()

# COMMAND ----------


