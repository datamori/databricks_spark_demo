# Databricks notebook source
# MAGIC %fs
# MAGIC ls /mnt/mydatalake2

# COMMAND ----------

import os
import datetime
ts = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
#print(ts)
os.rename("/dbfs/mnt/mydatalake2/ElectricVehiclePopulationData.json", "/dbfs/mnt/mydatalake2/archives/ElectricVehiclePopulationData.json"+"_"+ts)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/mydatalake2/archives

# COMMAND ----------


