#!/usr/bin/env python
# coding: utf-8

# # withColumn

# * `withColumn()` 是 DataFrame 裡的 transformation function，常被用來. 
#   * change the value. 
#   * convert the datatype. 
#   * create a new column
#   * many more
# * 先來做個範例資料

# In[8]:


data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]


from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
df = spark.createDataFrame(data=data, schema = columns)
df.show()


# ## change DataType

# * 改欄位類型，要用 withColumn 搭配 `.cast()`，直接看範例

# In[5]:


df.withColumn("salary", F.col("salary").cast("Integer")).show()


# ## update the value of an existing column

# In[9]:


df.withColumn("salary",F.col("salary")*100).show()


# ## Create a column from an existing

# In[11]:


df.withColumn("CopiedColumn",F.col("salary")* -1).show()


# ## Create a column with constant value

# * 如果新增的欄位，要給他統一的值(constant value)，要用到 `lit()` 這個 function

# In[13]:


df.withColumn("Country", F.lit("USA")).show()


# In[16]:


df.withColumn("Country", F.lit("USA"))   .withColumn("anotherColumn", F.lit("anotherValue"))   .show()


# In[ ]:




