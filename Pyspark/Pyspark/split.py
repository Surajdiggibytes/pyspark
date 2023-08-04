from pyspark.sql import SparkSession
from pyspark.sql.functions import col,split
spark = SparkSession.builder.getOrCreate()

data = [(1,'name1,name2'),(2,'name3,name4'),(3,'name5,name6')]
schm = ['sl_no','name']
df= spark.createDataFrame(data,schm)
df2 = df.withColumn('name',split(col('name'),',')),
       
df2.show()