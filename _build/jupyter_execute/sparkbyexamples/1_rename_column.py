#!/usr/bin/env python
# coding: utf-8

# # Rename Column

# ## 範例資料

# In[9]:


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

dataDF = [(('James','','Smith'),'1991-04-01','M',3000),
  (('Michael','Rose',''),'2000-05-19','M',4000),
  (('Robert','','Williams'),'1978-09-05','M',4000),
  (('Maria','Anne','Jones'),'1967-12-01','F',4000),
  (('Jen','Mary','Brown'),'1980-02-17','F',-1)
]

schema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('dob', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

df = spark.createDataFrame(data = dataDF, schema = schema)
df.printSchema()


# ## withColumnRenamed (one column)

# * 把 `dob` 這個 column 的名稱，換成 `DateOfBirth`. 

# In[2]:


df.withColumnRenamed("dob","DateOfBirth").printSchema()


# ## withColumnRenamed (multiple column)

# * 要改多個欄位，就必須重複多寫幾次

# In[3]:


df      .withColumnRenamed("dob","DateOfBirth")     .withColumnRenamed("salary","salary_amount")     .printSchema()


# ## rename nested column

# * 如果要 rename nested column (例如 name 底下的 firstname, middlename, lastname)，必須重寫 schema，然後塞進去

# In[10]:


schema2 = StructType([
    StructField("fname",StringType()),
    StructField("middlename",StringType()),
    StructField("lname",StringType())
])

df.select(
    F.col("name").cast(schema2), \
    F.col("dob"),
    F.col("gender"),
    F.col("salary")) \
   .printSchema()


# * 另一種做法，是像 sql 那樣，用 select 的，然後幫他用別名的方式做設定。但缺點是，資料的結構消失，變成 flat table

# In[12]:


df.select(
    F.col("name.firstname").alias("fname"),
    F.col("name.middlename").alias("mname"),
    F.col("name.lastname").alias("lname"),
    F.col("dob"),
    F.col("gender"),
    F.col("salary")
) \
  .printSchema()

