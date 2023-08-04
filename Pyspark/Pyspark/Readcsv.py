from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read.format("csv").load('C:\Users\SurajJ\PycharmProjects\spark\Csvfiles\file1.csv')
df.show()