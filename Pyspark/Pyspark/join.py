from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
data = [(1,'abc',25000),(2,'bbc',26000),(3,'acc',27000),(4,'bba',28000)]
schema= ['slno','Name','Salary']
data1 = [(1,'Add1','DE'),(2,'ADD2','DE'),(3,'ADD3','FS'),(4,'ADD4','FS')]
schm = ['slno','Address','Designation']
df = spark.createDataFrame(data,schema)
df1= spark.createDataFrame(data1,schm)

df.join(df1,df.slno==df1.slno,'full').show()
