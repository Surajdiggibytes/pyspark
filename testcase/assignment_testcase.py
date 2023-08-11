import unittest
from pyspark.sql import SparkSession
from src.pyspark.pyspark_assignment1 import util_assignment
class MyTestCase(unittest.TestCase):
    def test_something(self):
        spark =SparkSession.builder.getOrCreate()
        # Test case input dataframe
        data = [('Washing Machine', 1648770933000, 2000, 'Samsung', 'India', '001'),
                ('Refeigerator', 1648770999000, 35000, '  LG', None, '002'),
                ('Air Cooler', 1648770948000, 45000, '  Voltas', None, '003')]
        schema = ['Product Name', 'IssueDate', 'Price', 'Brand', 'Country', 'product_number']


        df = spark.createDataFrame(data, schema)

        # output dataframe
        dataoutput1= [('Washing Machine', 1648770933000, 2000,'Samsung','India', '001','2022-04-01T05:25:33.000+0530','2022-04-01','Samsung'),
                ('Refeigerator', 1648770999000, 35000, '  LG','Empty', '002','2022-04-01T05:26:39.000+0530','2022-04-01','LG'),
                ('Air Cooler', 1648770948000, 45000,'  Voltas', 'Empty', '003','2022-04-01T05:25:48.000+0530','2022-04-01','Voltas')]

        # Define the schema for the DataFrame
        schemaoutput1 = ['Product Name', 'IssueDate', 'Price','Brand','Country', 'product_number','NewTime','Date','Brands']
        test1output_df =spark.createDataFrame(dataoutput1, schemaoutput1)
        output_df = util_assignment.Timestamp(df)
        # test1output_df.show()
        # output_df.show()



        self.assertEqual(output_df.collect(), test1output_df.collect())  # add assertion here


if __name__ == '__main__':
    unittest.main()
