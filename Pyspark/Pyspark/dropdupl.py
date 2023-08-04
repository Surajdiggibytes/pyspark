from pyspark.sql import SparkSession
from pyspark.sql.functions import when
spark = SparkSession.builder.getOrCreate()
data =[(1,'abc','M',20000),(2,'acb','F',30000),(3,'adb','M',35000),(3,'adb','M',35000)]
schema = ['slno','Name','Gender','Salary']
df = spark.createDataFrame(data,schema)
df2 = df.dropDuplicates()
df2.show()