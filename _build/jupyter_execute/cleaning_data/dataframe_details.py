#!/usr/bin/env python
# coding: utf-8

# # DataFrame details

# In[14]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


# ## Intro to data cleaning with Apache Spark

# * 資料清理要做的事情包括：  
#   * reformating and/or replacing texts
#   * reforming calculations
#   * removing garbage or incomplete data  
# * Problems with typical data systems:  
#   * Performance. 
#   * Organizing data flow  
# * Spark 的優勢. 
#   * Scalable. 
#   * Powerful framework for data handling. 
# * Spark Schemas. 
#   * 可用來定義 DataFrame 的 format(i.e. 各個欄位的資料類型: Strings, dates, integers, arrays.)  
#   * 可以在 import 階段，就濾掉 garbage data
#   * 可以增加讀檔效率

# In[ ]:


from pyspark.sql.types import *

peopleSchema = StructType([
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('city', StringType(), True),
])

people_df = spark.read.format('csv').load(name="rawdata.csv", schema = peopleSchema)


# ## Using lazy processing

# In[86]:


aa_dfw_df = spark.read.format('csv').options(Header=True).load('data/AA_DFW_2017_Departures_Short.csv.gz')


# In[87]:


get_ipython().run_cell_magic('time', '', "aa_dfw_df = aa_dfw_df \\\n    .withColumn('airport', F.lower('Destination Airport')) \\\n    .withColumnRenamed('Date (MM/DD/YYYY)', 'date') \\\n    .withColumnRenamed('Flight Number', 'flight_number') \\\n    .withColumnRenamed('Actual elapsed time (Minutes)', 'actual_elapsed_time') \\\n    .drop('Destination Airport')")


# In[88]:


get_ipython().run_cell_magic('time', '', 'aa_dfw_df.show()')


# * 可以注意到，前面在做 transformation 的每個步驟時，處理時間都超短(因為他只記錄你的recipe，沒有真的執行). 
# * 直到 show data 時，他才一次執行

# ## Understanding Parquet

# * 當我們剛開始接觸 spark 的時候，常常都是先從 csv, json 這種格式來讀資料。這些格式可以彈性支援不同類型的資料，但對 spark 來說，並不是最好的格式。  
# * 舉例來說，CSV file 具有以下缺點. 
#   * 沒有幫我們定義 schema. 
#   * 如果是 nested data，需要特殊處理. 
#   * encoding format limited  
# * 以至於用 spark 去讀 csv 時，會出現：  
#   * slow to parse. 
#   * 沒辦法讀取部分資料就好 (無法在讀取時做filter，專業術語叫 no "predicate pushdown")  
# * Parquet format. 
#   * 是一種 columnar data format, Spark和其他 data processing frameworks都有支援這種格式
#   * 有自動存取 schema information  
#   * supports predicate pushdown (不用讀完一整張資料).  
# * 讀檔和寫檔的範例如下  
# 
# ```
# # read
# df = spark.read.format("parquet").load("filename.parquet")
# df = spark.read.parquet("filename.parquet")
# 
# # write
# df.write.format("parquet").save("filename.parquet")
# df.write.parquet("filename.parquet")
# ```

# * 我們這邊來練習，先把剛剛已讀進來的資料寫出成 parquet 檔，再讀進來

# In[89]:


aa_dfw_df.write.parquet("data/aa_dfw_all.parquet")


# In[90]:


aa_dfw_df_from_parquet = spark.read.parquet("data/aa_dfw_all.parquet")


# In[91]:


aa_dfw_df_from_parquet.count()


# ## 將資料註冊到 TempView 後，可直接用 sql 指令

# In[105]:


# 將 spark DataFrame 註冊到 TempView 裡面
aa_dfw_df_from_parquet.createOrReplaceTempView('aa_dfw_view') # 註冊的名字自己取

# 可以下 sql 語法了
query_res = spark.sql('SELECT avg(actual_elapsed_time) from aa_dfw_view')
query_res.show()


# # Querys

# In[ ]:


# select
voters = voter_df.select('name', 'position')
voters = voter_df.select(voter_df.name) # 抓出 name 這個欄位

# filter
voter_df.filter(voter_df.name.like("M%")) # 抓出 name 裡面有 ”M" 開頭的字
voter_df.filter(voter_df['name'].isNotNull()) # 抓出 name 裡面不是 null 的值
voter_df.where(~voter_df._c1.isNull()) # 抓出 _c1 裡面不是 null 的值 (~ 就是 R 的 !)
voter_df.filter(voter_df.date > '1/1/2019') # 抓出 date 裡面大於 '1/1/2019' 的值
voter_df.filter(voter_df.date.year > 1800)


# mutate (i.e. withColumn)
import pyspark.sql.functions as F
voter_df.withColumn('year', voter_df.date.year) # 新增 year 這個欄位，內容就是 voter_df.date.year
voter_df.withColumn('name_upper_case', F.upper('name'))
voter_df.withColumn('splits', F.split('name', ' '))
voter_df.withColumn('year', voter_df['_c4'].cast(IntegerType()))

# drop
voter_df.drop('unused_column') # 刪掉 "unused_column". 

# distinct
voter_df.select(voter_df['VOTER_NAME']).distinct()


# In[124]:


voter_df = spark.read.format('csv').options(Header=True).load('data/DallasCouncilVoters.csv.gz')
voter_df.show()


# In[113]:


# 找出 distinct 的 VOTER_NAME
voter_df.select(voter_df['VOTER_NAME']).distinct().show(40, truncate=False)


# In[125]:


# Filter voter_df where the VOTER_NAME is 1-20 characters in length
voter_df = voter_df.filter('length(VOTER_NAME) > 0 and length(VOTER_NAME) < 20')


# In[126]:


# Filter out voter_df where the VOTER_NAME contains an underscore
voter_df = voter_df.filter(~ F.col('VOTER_NAME').contains('_'))


# In[127]:


# Show the distinct VOTER_NAME entries again
voter_df.select('VOTER_NAME').distinct().show(40, truncate=False)


# In[128]:


# 把 voter_name 依照空白切開 ( \s+ 是正規表達式，表 Space or multiple space)
voter_df = voter_df     .select('VOTER_NAME')     .distinct()     .withColumn("splits", F.split(voter_df.VOTER_NAME, '\s+'))

voter_df.show(40, truncate=False)


# In[133]:


# splits欄位的第一個element當 first_name, 最後一個 element 當 last_name
voter_df = voter_df     .withColumn("first_name", voter_df.splits.getItem(0))     .withColumn("last_name", voter_df.splits.getItem(F.size(voter_df.splits)-1))
voter_df.show()


# ### conditional

# * 就是 inline 版本的 if...else，但他的語法是 F.when()

# In[135]:


# first name 是 Scott 的，我給他 100 分
voter_df     .withColumn('random_val', F.when(voter_df.first_name == 'Scott', 100))     .show()


# In[147]:


# first name 是 Scott 的，我給他 100 分，last name 是 Young 的，我給他 200 分
voter_df     .withColumn('random_val', 
                F.when(voter_df.first_name == 'Scott', 100) \
                .when(voter_df.last_name == "Young", 200)) \
    .show()


# In[148]:


# 其他的，我給他 0~1 的 random number
voter_df     .withColumn('random_val', 
                F.when(voter_df.first_name == 'Scott', 100) \
                .when(voter_df.last_name == "Young", 200) \
                .otherwise(F.rand())) \
    .show()


# In[ ]:


df.select(df.Name, df.Age, F.when(df.Age>=18, "Adult"))
df.select(df.Name, df.Age, 
          F.when(df.Age>=18, "Adult")
         F.when())

df.select(df.Name, df.Age, 
          F.when(df.Age>=18, "Adult")
         F.otherwise("Minor"))

