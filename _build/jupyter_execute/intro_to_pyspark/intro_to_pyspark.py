#!/usr/bin/env python
# coding: utf-8

# # Intro

# ## 什麼是 Spark

# * Spark是用來做cluster computing的一個平台(platform)  
# * 實務上來說，cluster會被 hosted 在一個遠端的電腦(稱為 master)，他的任務是做資料分割和計算。  
# * master 會和 cluster 裡面的其他電腦連接再一起(這些電腦叫 worker)，master會把data切割成好幾份資料，寄送給workers，workers做完計算後，再把結果送回給master統整。  
# * 所以，我們把原本超巨大的資料，切成好n塊，丟到n個nodes中幫我們計算，就可以處理掉big data的問題，因為每個node其實只要處理小小的資料就好  
# * 所以，both data processing and computation are performed in parallel over the nodes in the cluster  
# * 所以，有需要用spark的兩個條件是：  
#   * 我的資料實在大到一台電腦無法處理，要綁多台電腦一起處理才行  
#   * 我想做的運算，是可以平行化處理的，我才能把工作分散到多個電腦中一起做  

# ## 使用 Spark (RDD vs DataFrame)

# * spark 的核心資料結構是 Resilient Distributed Dataset (RDD).  
# * 這是一個低階物件，可讓 Spark 將資料切割到多個 nodes 去計算.   
# * 要使用 RDD 的話，要先instance 一個 `SparkContext` 物件 (e.g. `sc = pyspark.SparkContext(master = local[2])`)。之後就可以用 sc 這個物件，去 load/create RDD 資料，之後就可以做運算了。  
# * 但 RDDs 不太好寫。比如說，要達到同一個目的，你可以有多種寫法，但效能差異很大，你必須很有sense的選擇好的寫法，才會有好的performance. 
# * 所以，後來又推出了 `Spark DataFrame` 這種資料結構。他是建立在 RDDs 上面的資料結構，被設計的像 SQL table，不只好理解，好寫，而且效能已經被最佳化的很好了  
# * 要使用 `Spark DataFrame` 的話，要先 instance 一個 `SparkSession` 物件 (e.g. `spark = pyspark.sql.SparkSession.builder.getOrCreate()`)，之後就可以用 spark 這個物件，去 create DataFrame, register DataFrame as tables, execute SQL over tables, cache tables, and read parquet files.

# ### 建立 SparkSession

# In[31]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print(spark)


# ### 讀檔 (轉換成 Spark DataFrame)

# * 要建立 Spark DataFrame 的方式有很多，我們先介紹最簡單的：讀 local 的 csv 檔

# In[32]:


flights = spark.read.csv("flights_small.csv") # 此時的 flights，已經是 Spark DataFrame 這種資料結構
flights


# * 可以看到，當我想 print `flights` 時，他並沒有真的 print 出一個 table 給我。
# * 因為 Spark DataFrame 是 lazy evaluztion，所以他只會給你看基本資訊，直到你叫他 show 出結果時，他才會真的執行：

# In[33]:


flights.show(5) # show 前五筆


# In[35]:


flights.select("_c0").show()


# ### View tables

# * 一但建立好 SparkSession 後，就可以看看我們的 cluster 裡面已經有哪些 data 可以用了. 
# * SparkSession有個 attribute 叫 `catalog`， 裡面就會列出 cluster 內有的所有 data  
# * This attribute has a few methods for extracting different pieces of information.
# * One of the most useful is the .listTables() method, which returns the names of all the tables in your cluster as a list.

# ## 架構

# ![](spark_figure.png)

# * 如上圖，那朵大大的雲，就是Spark的世界，我們想要運用它強大的平行運算能力。那第一步，就是先去和這朵雲建立 connection. 
# * 
# 
# 我們從 `user` 和上面的 `SparkContext`，以及右邊的 `SparkSession` 開始看
# * 

# In[4]:





# ## spark DataFrame 轉成 pandas DataFrame

# * 當我們在 spark 上把資料篩選成比較小的資料集時，就可以把它轉成 pandas ，就有更多工具可以做處理了

# In[ ]:


query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"

# Run the query
flight_counts = spark.sql(query)

# Convert the results to a pandas DataFrame
pd_counts = flight_counts.toPandas()

# Print the head of pd_counts
print(pd_counts)


# ## pandas DataFrame 轉成 spark DataFrame

# In[16]:


import pandas as pd
import numpy as np


# In[21]:


pd_temp = pd.DataFrame(np.random.random(10), columns = ["col1"])

# Create spark_temp from pd_temp
spark_temp = spark.createDataFrame(pd_temp)


# In[22]:


# Examine the tables in the catalog
print(spark.catalog.listTables())

# Add spark_temp to the catalog
spark_temp.createOrReplaceTempView("temp")

# Examine the tables in the catalog again
print(spark.catalog.listTables())


# In[24]:


pd_temp


# In[27]:


spark.sql("select col1 from temp where col1 > 0.5").show()


# In[ ]:




