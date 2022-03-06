#!/usr/bin/env python
# coding: utf-8

# # Join Two DataFrames

# * 先來做兩個範例資料

# In[2]:


from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

emp = [(1,"Smith",-1,"2018","10","M",3000),     (2,"Rose",1,"2010","20","M",4000),     (3,"Williams",1,"2010","10","M",1000),     (4,"Jones",2,"2005","10","F",2000),     (5,"Brown",2,"2010","40","",-1),       (6,"Brown",2,"2010","50","",-1)   ]
empColumns = ["emp_id","name","superior_emp_id","year_joined",        "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10),     ("Marketing",20),     ("Sales",30),     ("IT",40)   ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)


# * 可以看到，這兩張表，要透過 上表的`emp_dept_id` 和 下表的`dept_id` 做串接 

# ## inner join

# In[13]:


empDF.join(deptDF, 
           on = empDF.emp_dept_id ==  deptDF.dept_id, 
           how = "inner") \
     .show(truncate=False)


# ## full (outer) join

# In[14]:


empDF.join(deptDF,
           on = empDF.emp_dept_id ==  deptDF.dept_id,
           how = "full") \
    .show(truncate=False)

# empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"outer") \
#     .show(truncate=False)

# empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"fullouter") \
#     .show(truncate=False)


# ## left (outer) join

# In[15]:


empDF.join(deptDF, 
           on = empDF.emp_dept_id ==  deptDF.dept_id,
           how = "left").show(truncate = False)
# empDF.join(deptDF, empDF.emp_dept_id ==  deptDF.dept_id,"leftouter").show(truncate = False)


# ## right (outer) join

# In[16]:


empDF.join(deptDF,
           on = empDF.emp_dept_id ==  deptDF.dept_id,
           how = "right") \
   .show(truncate=False)
# empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"rightouter") \
#    .show(truncate=False)


# ## left semi join

# * 意思是，做完 inner join 後，只保留 left table 的欄位

# In[17]:


empDF.join(deptDF,
           on = empDF.emp_dept_id ==  deptDF.dept_id,
           how = "leftsemi") \
   .show(truncate=False)


# ## left anti join

# * 意思是，只保留左表有，但右表沒有的資料

# In[18]:


empDF.join(deptDF,
           on = empDF.emp_dept_id ==  deptDF.dept_id,
           how = "leftanti") \
   .show(truncate=False)


# ## key 是多個欄位的話

# * 這邊沒有舉範例資料，但重點就是用 list 串起來就好：

# ```
# # 兩邊的 key 名稱不同時
# df1.join(df2,
#            on = [df1.key1 == df2.key1,
#                  df1.key2 == df2.key2],
#            how = "inner") \
#    .show(truncate=False)
# 
# # 兩邊的 key 名稱相同時
# df1.join(df2,
#            on = [key1, key2],
#            how = "inner") \
#    .show(truncate=False)
# ```

# ## self join

# * 來看一下原本的 employee 資料

# In[21]:


empDF.show(truncate=False)


# * 裡面的 superior_emp_id 是指該員工的上司的工號. 
# * 但我這邊如果想知道id=2的上司，我就要用 emp_id == 2 來找他的 name，實在麻煩
# * 這時候，我可以用原本的表，再生出只有 `emp_id`和`name` 這兩個欄位的新表，然後把原表的 superior_emp_id 和新表的 emp_id 做 join，就可以把上司的名字給 join 過來了

# In[28]:


emp1 = empDF
emp2 = empDF     .select("emp_id", "name")     .withColumnRenamed("name", "superior_emp_name")     .withColumnRenamed("emp_id", "superior_emp_id")

emp1.join(
    emp2,
    on = "superior_emp_id",
    how = "inner"
    ) \
    .select("emp_id", "name", "superior_emp_id", "superior_emp_name") \
    .show(truncate=False)


# * 你也可以做得帥一點，一次到位的完成：

# In[20]:


empDF.alias("emp1").join(empDF.alias("emp2"),     F.col("emp1.superior_emp_id") == F.col("emp2.emp_id"),"inner")     .select(F.col("emp1.emp_id"),F.col("emp1.name"),       F.col("emp2.emp_id").alias("superior_emp_id"),       F.col("emp2.name").alias("superior_emp_name"))    .show(truncate=False)


# ## 使用 sql 語句

# * 老招，把這兩張表，都註冊到 TempView 後，可以直接下 sql

# In[29]:


empDF.createOrReplaceTempView("EMP")
deptDF.createOrReplaceTempView("DEPT")

joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id")   .show(truncate=False)

joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id")   .show(truncate=False)


# ## join 多張表

# * 就一路串下去就好，就不提供範例了，給 sample code 就好. 
# 
# ```
# df1.join(df2,df1.id1 == df2.id2,"inner") \
#    .join(df3,df1.id1 == df3.id3,"inner")
# ```
