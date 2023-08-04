from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.getOrCreate()
data=[(1,'abc'),(2,'acb'),(3,'adb'),(4,'aed')]

Schema = StructType([StructField(name='sl_no',dataType=IntegerType()),
                     StructField(name='Name',dataType=StringType())])

df= spark.createDataFrame(data,['id','name'])
#df= spark.createDataFrame(data=data,schema=schema)
df.show()