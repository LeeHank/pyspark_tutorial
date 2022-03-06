#!/usr/bin/env python
# coding: utf-8

# # filter

# * `filter()` 就是 sql 裡的 `where()`，等等的語法，你要用 filter 或用 where 都是完全互通的 (我喜歡用 filter，比較有 dplyr 的親切感)

# In[5]:


from pyspark.sql.types import StructType,StructField 
from pyspark.sql.types import StringType, IntegerType, ArrayType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
 ]
        
schema = StructType([
     StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
     ])),
     StructField('languages', ArrayType(StringType()), True),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
 ])

df = spark.createDataFrame(data = data, schema = schema)
df.printSchema()
df.show(truncate=False)


# ## filter() with one column

# In[11]:


# 篩選出 state 為 OH 的列
df.filter(df.state == "OH").show(truncate=False)
df.filter(df["state"] == "OH").show(truncate=False)
df.filter(F.col("state") == "OH").show(truncate=False) # 用 col() 也可標出欄位
df.filter("state == 'OH'").show(truncate=False) # sql 寫法


# In[15]:


# 篩選出 state 不為 OH 的列
df.filter(df.state != "OH").show(truncate=False)
df.filter(~(df.state == "OH")).show(truncate=False)
df.filter("state <> 'OH'").show(truncate=False) # sql 寫法


# ## filter with multiple column

# In[16]:


df.filter( (df.state  == "OH") & (df.gender  == "M") )     .show(truncate=False) 


# ## `.isin(List)`. 

# In[18]:


#Filter IS IN List values
df.filter(df.state.isin(["OH","CA","DE"])).show()
df.filter(~df.state.isin(["OH","CA","DE"])).show()


# ## startswith(), endwith(), contains()

# In[19]:


df.filter(df.state.startswith("N")).show()


# In[21]:


df.filter(df.state.endswith("H")).show()


# In[22]:


df.filter(df.state.contains("H")).show()


# ## like & rlike

# * 先換個範例資料

# In[25]:


data2 = [(2,"Michael Rose"),(3,"Robert Williams"),
     (4,"Rames Rose"),(5,"Rames rose")]
df2 = spark.createDataFrame(data = data2, schema = ["id","name"])
df2.show()


# * like 就是 sql 的那個 like，看例子：

# In[26]:


df2.filter(df2.name.like("%rose%")).show()


# In[27]:


# rlike - SQL RLIKE pattern (LIKE with Regex)
#This check case insensitive
df2.filter(df2.name.rlike("(?i)^*rose$")).show()


# ## 篩選 column type 是 array 的欄位

# * 例如剛剛的例子， languages 就是一個 array 型的欄位

# In[29]:


df.show()


# * 那我如果想篩出， languages 有出現 Java 的列，我可以這樣做

# In[30]:


df.filter(F.array_contains(df.languages, "Java")).show(truncate=False)


# ## 篩選 nested struct column

# * 如果想篩選 nested column，可以一路指到他。例如剛剛 df 的結構：

# In[31]:


df.printSchema()


# * 我如果想篩出，lastname為 Williams 的列，我就這樣做就好

# In[32]:


df.filter(df.name.lastname == "Williams")     .show(truncate=False)


# In[ ]:




