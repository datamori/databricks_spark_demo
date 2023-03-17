#!/usr/bin/env python
# coding: utf-8

# In[66]:


from snowflake.snowpark import *
import snowflake.snowpark.functions as F


# In[67]:


connection_parameters = {
    "account": "whpxseb-ke63785",
    "user": "zenken",
    "password": "Josh0814!",
    "role": "ACCOUNTADMIN",
    "warehouse": "MYWH2",
    "database": "EV_POP_DATA2",
    "schema": "EV_POP_SCHEMA"}

db_table = 'RAW_DATA'


# In[68]:


sf_session = Session.builder.configs(connection_parameters).create()


# In[69]:


snow_df = sf_session.sql('SELECT JSON_SRC:meta as meta FROM RAW_DATA').dropna()
snow_df.show()


# In[70]:


dfViewRaw = snow_df.select(snow_df.meta.getItem("view").alias("view"))
dfViewRaw.show()

#dfApprovals = dfViewRaw.select(F.col("view").getItem("approvals").alias("approvals"))
dfApprovals = dfViewRaw.selectExpr("view:approvals as approvals")
dfApprovals.show()


# In[ ]:




