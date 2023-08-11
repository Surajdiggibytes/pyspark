import datetime


from pyspark.sql.functions import col,from_unixtime,col,split,ltrim,lower,unix_timestamp

# Timestamp = df.withColumn('NewTime',to_utc_timestamp((df.IssueDate / 1000).cast('timestamp'),"UTC"))
# df_with_format = Timestamp.withColumn('FormattedUtcTimestamp', date_format('NewTime', 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ')).show(truncate=False)

# Time stamp creation
def Timestamp(df):
    Tf = df.withColumn('NewTime', from_unixtime(col('IssueDate') / 1000, "yyyy-MM-dd\'T\'HH:mm:ss.SSSZ"))

    #Time stamp to date format
    Daf = Tf.withColumn('Date',split(col('NewTime'),'T')[0])

#Trim space
    Tf = Daf.withColumn('Brands',ltrim(col('Brand')))
# #Removing null value
    Tf = Tf.fillna('Empty')#.show(truncate=False)
    return Tf


# The second column

#df2.show()

# # Camel to snakecase
#
def secondtable(df2):
    Rename = df2.withColumnRenamed('SourceId', 'source_id') \
        .withColumnRenamed('TransactionNumber', 'transaction_number') \
        .withColumnRenamed('Language', 'language') \
        .withColumnRenamed('ModelNumber', 'model_number') \
        .withColumnRenamed('StartTime', 'start_time') \
        .withColumnRenamed('ProductNumber', 'product_number')
    # # Rename.show(truncate=False)
    #
    #
    # #start_column
    #
    start_timestamp = Rename.withColumn('start_time',unix_timestamp(col('start_time'), "yyyy-MM-dd'T'HH:mm:ss.SSSZ") * 1000)
    start_timestamp#.show(truncate=False)
    return start_timestamp
#
# #combine both table
#
# def finaltable(Tf,start_timestamp):
#     joined_tables = Tf.join(start_timestamp, Tf.product_number == start_timestamp.product_number, "outer")#.show()