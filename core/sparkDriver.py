from pyspark.sql.types import *
from pyspark.sql import SparkSession
import sys

if __name__ == '__main__':
    spark_obj2 = SparkSession.builder\
        .appName('Export data to LT') \
        .getOrCreate()

    hdfs_csv_file_path = sys.argv[1]
    db_name = sys.argv[2]
    table_name = sys.argv[3]
    db_table = db_name + '.' + table_name

    #print hdfs_csv_file_path
    #print db_table

    dataframe = spark_obj2.read\
        .option('header', True) \
        .option('delimiter', '|') \
        .csv(hdfs_csv_file_path)
    if dataframe:
        dataframe.write.saveAsTable(db_table, mode='overwrite')
