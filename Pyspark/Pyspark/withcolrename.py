from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.getOrCreate()

schem= ['slno','Name','Salary','Designation']
data = [(1,'abc',30000,'DE'),(2,'bbc',31000,'DE'),(3,'cbc',32000,'DE'),(4,'dbc',33000,'DE'),(5,'ebc',34000,'DE')]
df = spark.createDataFrame(data,schem)
df2 = df.withColumn('New_salary',col('Salary'))
df3 = df2.withColumnRenamed('Salary','Current_salary')
df3.show()