from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()
# schem= ['slno','Name','Salary','Designation']
# data = [(1,'abc',30000,'DE'),(2,'bbc',31000,'DE'),(3,'cbc',32000,'DE'),(4,'dbc',33000,'DE'),(5,'ebc',34000,'DE')]
df = spark.read.format('csv').load('C:\\Users\\SurajJ\\PycharmProjects\\spark\\Csvfiles\\file1.csv',header =True)
# df2 = spark.createDataFrame(df)
df.show()
df.printSchema()

df2 = df.withColumn('year',col('year').cast('Integer'))\
    .withColumn('Industry_name_NZSIOC',col('Industry_name_NZSIOC').cast('integer'))

# df.printSchema()
df2.printSchema()