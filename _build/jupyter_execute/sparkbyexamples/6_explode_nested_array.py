#!/usr/bin/env python
# coding: utf-8

# # explode nested array into rows

# * 先做出 Nested array 的範例資料

# In[2]:


from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayArrayData = [
  ("James",[["Java","Scala","C++"],["Spark","Java"]]),
  ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
  ("Robert",[["CSharp","VB"],["Spark","Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjects'])

df.printSchema()
df.show(truncate=False)


# * 如果用之前學過的 `explode()` function，他只會幫你解掉一層，如下：

# In[3]:


df.select(df.name, F.explode(df.subjects)).show(truncate=False)


# * 用這章教的 `floatten()` function，則是幫你把 nested array，變成單一 array

# In[9]:


df.select(df.name, F.flatten(df.subjects).alias("subjects")).show(truncate=False)


# * 那使用 combo 技，就可以完全解開了：

# In[19]:


df     .select(df.name, F.flatten(df.subjects).alias("subjects"))     .select(F.col("name"), F.explode(F.col("subjects")).alias("subjects"))     .show()


# In[ ]:




