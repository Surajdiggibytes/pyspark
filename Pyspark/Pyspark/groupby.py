from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
data = [(1,'abc','1ADBC','IT'),(2,'bbc','2ADBC','HR'),(3,'adc','3ADBC','BI'),(4,'1stmain','3ADB','IT'),(5,'2ndmain','3ADB','IT')]
schema= ['sl_no','Name','ID','DEPT']
df = spark.createDataFrame(data,schema)
df2 = df.groupby(df.DEPT).count()
df2.show()