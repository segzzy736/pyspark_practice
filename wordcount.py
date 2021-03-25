import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSV_DF").master("local[*]").getOrCreate()

print("This is good")