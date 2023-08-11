from pyspark.sql import SparkSession
from src.pyspark.pyspark_assignment1 import util_assignment
spark = SparkSession.builder.getOrCreate()
data = [('Washing Machine',1648770933000,2000,'Samsung','India','001'),('Refeigerator',1648770999000,35000,'  LG',None,'002'),('Air Cooler',1648770948000,45000,'  Voltas',None,'003')]
schema = ['Product Name','IssueDate','Price','Brand','Country','product_number']
df = spark.createDataFrame(data,schema)
# second
data2 = [(150711,123456,'EN','456789','2021-12-27T08:20:29.842+0000','001'),(150439,234567,'UK','345678','2021-12-27T08:21:14.645+0000','002'),(150647,345678,'ES','234567','2021-12-27T08:22:42.445+0000','003')]
schema2=['SourceId','TransactionNumber','Language','ModelNumber','StartTime','ProductNumber']
df2 = spark.createDataFrame(data2,schema2)
# Timestamp
#util_assignment.Timestamp(df)
util_assignment.secondtable(df2)