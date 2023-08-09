from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
data = [(1,'abc','1ssb2'),(2,'abd','1ssb3'),(3,'abe','1ssb4')]
schema = ['slno','name','id']
def sampleopr(data,schema):
    df = spark.createDataFrame(data, schema)
    df1 = spark.range(1, 2)
    df2 = df1.sample(0.1, 123).show()