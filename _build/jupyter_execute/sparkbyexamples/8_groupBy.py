#!/usr/bin/env python
# coding: utf-8

# # groupBy()

# * 先來生範例資料

# In[23]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

# schema = StructType([
#     StructField('employee_name', StringType(), True),
#     StructField('department', StringType(), True),
#     StructField('state', StringType(), True),
#     StructField('salary', FloatType(), True),
#     StructField('age', DoubleType(), True),
#     StructField('bonus', DoubleType(), True)
# ])

schema = ["employee_name","department","state","salary","age","bonus"]

df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)


# ## groupBy() 後的資料格式

# * 做完 groupBy()後，他會return 一個 `GroupedData` object，這個 object 可以接續使用以下 aggregate methods:  
#   * `count()`: 算每組的個數. 
#   * `mean()`. 
#   * `max()`. 
#   * `min()`. 
#   * `sum()`. 
#   * `avg()`. 
#   * `agg()`: 這就可以做多個 summarise function. 
#   * `pivot()`: 之後介紹. 
# * 要注意的是， dplyr 的 groupby + filter，在這邊是不允許的... 之後再想辦法

# ## 對 n 組 做groupby()後，對 k 個 column，提供 "一種" summarise 方法

# * 直接看最複雜的例子，簡單的例子就依序簡化就好. 
# * 我們想對 department, state 做 groupby，然後對 salary 和 bonus，求最小值

# In[7]:


df     .groupBy("department", "state")     .min("salary", "bonus")     .show(truncate=False)


# * 所以，我如果分組的欄位，只要一個，例如 department 就好，那就改成

# In[8]:


df     .groupBy("department")     .min("salary", "bonus")     .show(truncate=False)


# * 那如果我想 summarise 的欄位，也只要一個，salary就好，那就改成：

# In[16]:


df     .groupBy("department")     .min("salary")     .show(truncate=False)


# ## 用 `agg()` 提供多個 summarise function 一起做

# In[33]:


df.groupBy("department")     .agg(
        F.sum("salary").alias("sum_salary"), \
        F.mean("salary").alias("avg_salary"), \
        F.sum("bonus").alias("sum_bonus"), \
        F.max("bonus").alias("max_bonus")
    ) \
    .show(truncate=False)


# ## 對 aggregate 後的資料做 filter

# * 就像 sql 在 groupby+aggregate後，用 having() 來做filter一樣，這邊的語法就不用變(不用改成 having)，直接用 filter 就好:

# In[34]:


df.groupBy("department")     .agg(
        F.sum("salary").alias("sum_salary"), \
        F.mean("salary").alias("avg_salary"), \
        F.sum("bonus").alias("sum_bonus"), \
        F.max("bonus").alias("max_bonus")
    ) \
    .where(F.col("sum_bonus") >= 50000) \
    .show(truncate=False)

