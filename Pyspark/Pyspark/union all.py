from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
data = [(1,'abc','1ADBC'),(2,'bbc','2ADBC'),(3,'adc','3ADBC')]
data2= [(4,'1stmain','3ADB'),(5,'2ndmain','3ADB')]
schm2= ['Address','Salary']
schema= ['sl_no','Name','ID']
df = spark.createDataFrame(data,schema)
df1 = spark.createDataFrame(data2,schema)

df2 = df.union(df1)
df2.show()