from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
spark = SparkSession.builder.getOrCreate()
data = [(1,3,5),(2,4,6),(3,8,9),(4,5,6)]
schema = ['slno','male','female']
df = spark.createDataFrame(data,schema)
df1 = df.select('slno',expr("stack(2,'male',male,'female',female) as(gender,count)")).show()
