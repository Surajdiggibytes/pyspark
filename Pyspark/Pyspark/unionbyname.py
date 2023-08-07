from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
data = [(1,'abc',25000)]
schema = ['slno','name','salary']
data2 = [(2,'aab',15000)]
schm = ['slno','name','amount']
df = spark.createDataFrame(data,schema)
df1 = spark.createDataFrame(data,schm)
df2=df.unionByName(df1,allowMissingColumns=True)
df2.show()