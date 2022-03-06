#!/usr/bin/env python
# coding: utf-8

# # explode array and map columns to rows

# * 有些表格，是 array or list column，

# In[2]:


import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})]

df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df.printSchema()
df.show(truncate = False)


# ## 用 explode() 把 array column 給攤平

# In[4]:


df2 = df.select(df.name, F.explode(df.knownLanguages))
df2.printSchema()
df2.show()


# ## 用 explode() 把 dictionary column 給攤平成 key, value

# In[6]:


df3 = df.select(df.name, F.explode(df.properties))
df3.printSchema()
df3.show()


# ## 用 explode_outer() 把 null 和 空格都保留下來

# In[7]:


df.select(df.name,F.explode_outer(df.knownLanguages)).show()


# In[9]:


df.select(df.name, F.explode_outer(df.properties)).show()


# ## 用 poseexplode() 把原本的 position 也存出來

# In[10]:


df.select(df.name,F.posexplode(df.knownLanguages)).show()


# In[11]:


df.select(df.name, F.posexplode(df.properties)).show()


# ## 用 posexplode_outer() 把 null 和空白都留下來

# * 原本的資料，在 `knownLanguages` 的第四列，是 null，所以如果你下 `poseexplode()` 時，他根本也無法存第四列的 position，他就會濾掉

# In[14]:


df.show(truncate = False)


# * 那現在改用 `posexplode_outer()`，就可以保留這列：

# In[13]:


df.select("name", F.posexplode_outer("knownLanguages")).show()


# * 同理，用在 dictionary 欄位也一樣：

# In[16]:


df.select(df.name, F.posexplode_outer(df.properties)).show()


# In[ ]:




