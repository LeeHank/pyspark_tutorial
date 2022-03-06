#!/usr/bin/env python
# coding: utf-8

# # orderBy() and sort()

# In[3]:


from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = [("James","Sales","NY",90000,34,10000),     ("Michael","Sales","NY",86000,56,20000),     ("Robert","Sales","CA",81000,30,23000),     ("Maria","Finance","CA",90000,24,23000),     ("Raman","Finance","CA",99000,40,24000),     ("Scott","Finance","NY",83000,36,19000),     ("Jen","Finance","NY",79000,53,15000),     ("Jeff","Marketing","CA",80000,25,18000),     ("Kumar","Marketing","NY",91000,50,21000)   ]
columns= ["employee_name","department","state","salary","age","bonus"]

df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)


# ## sort() 一個 or 多個欄位 (預設是 asscending)

# In[4]:


df.sort("department","state").show(truncate=False)
# df.sort(F.col("department"),F.col("state")).show(truncate=False) # 效果一樣


# ## 指定 asscending 或 descending

# In[5]:


df.sort(df.department.asc(),df.state.desc()).show(truncate=False)
# df.sort(F.col("department").asc(),F.col("state").desc()).show(truncate=False)


# ## 用純 sql 來寫

# In[6]:


df.createOrReplaceTempView("EMP") # 先把這張 table 註冊到 TempView 裡面
spark.sql("select employee_name, department, state, salary, age, bonus from EMP ORDER BY department asc").show(truncate=False)


# In[ ]:




