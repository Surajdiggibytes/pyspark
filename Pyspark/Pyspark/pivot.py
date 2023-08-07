from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
data = [(1,'IT','Male'),(2,'IT','Female'),(3,'HR','Male'),(4,'HR','Male')]
schema = ['slno','DEPT','Gender']
df = spark.createDataFrame(data,schema)
df2 =df.groupby('Gender')
df2.show()