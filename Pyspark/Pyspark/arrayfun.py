from pyspark.sql import SparkSession
from pyspark.sql.functions import array,col
spark = SparkSession.builder.getOrCreate()

data = [(1,'name1','name2'),(2,'name3','name4'),(3,'name5','name6')]
schm = ['sl_no','name','name2']

df = spark.createDataFrame(data,schm)
df2 = df.withColumn('New_column',array(col('name'),col('name2')))
df2.show()