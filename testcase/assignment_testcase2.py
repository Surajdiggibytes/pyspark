import unittest
from src.pyspark.pyspark_assignment1 import util_assignment
from pyspark.sql import SparkSession
from src.pyspark.pyspark_assignment1 import util_assignment

class MyTestCase(unittest.TestCase):
    def test_something(self):
        spark = SparkSession.builder.getOrCreate()
        datainput = [(150711, 123456, 'EN', 456789, '2021-12-27T08:20:29.842+0000', '001'),
                 (150439, 234567, 'UK', 345678, '2021-12-27T08:21:14.645+0000', '002'),
                 (150647, 345678, 'ES', 234567, '2021-12-27T08:22:42.445+0000', '003')]
        schemainput = ['SourceId', 'TransactionNumber', 'Language', 'ModelNumber', 'StartTime', 'ProductNumber']
        df_input = spark.createDataFrame(datainput, schemainput)

        # output
        dataoutput = [(150711,123456,'EN',456789,1640593229000,'001'),
                      (150439,234567,'UK',345678,1640593274000,'002'),
                      (150647,345678,'ES',234567,1640593362000,'003')]
        schemaoutput = ['source_id','transaction_number','language','model_number','start_time','product_number']
        df_output = spark.createDataFrame(dataoutput,schemaoutput)
        function_df= util_assignment.secondtable(df_input)



        self.assertEqual(function_df.collect(),df_output.collect())  # add assertion here


if __name__ == '__main__':
    unittest.main()
