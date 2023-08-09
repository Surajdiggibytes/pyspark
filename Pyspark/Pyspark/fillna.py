from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
data = [(1,'abc',None),(2,None,'Male'),(3,'abb','Female')]
schema = ['slno','Name','Gender']
df = spark.createDataFrame(data,schema)
df2 = df.fillna('Unknown',['Name',"Gender"]).show()