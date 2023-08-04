from pyspark.sql import SparkSession
from pyspark.sql.functions import col,array_contains
spark = SparkSession.builder.getOrCreate()

data = [(1,['name1','name2']),(2,['name3','name4']),(3,['name5','name6'])]
schm = ['sl_no','name']

df = spark.createDataFrame(data,schm)
df2 = df.withColumn('namess',array_contains(col('name'),'name1'))
df2.show()