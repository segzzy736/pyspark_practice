import os
from pyspark.sql import SparkSession

from pyspark import SparkConf, SQLContext, HiveContext, SparkContext

os.environ["SPARK_HOME"] = "/usr/local/hadoop-ecosystem/spark-3.0.1-bin"
spark = SparkSession.builder.appName("Mysql_Spark_DF").master("local[*]").enableHiveSupport().config("spark.jars","/usr/local/hadoop-ecosystem/spark-3.0.1-bin/jars/mysql-connector-java-5.1.48.jar").getOrCreate()

mysql_tables=["OHLC_prices", "email_receipts", "financial_statements", "human_capital", "HM"]
for tab in mysql_tables:

     table1 = tab
     table = "SELECT * FROM test." + table1 + "a"
     table2 = table1 + "b"

     df = spark.sql(table)
     df.write.format("orc").saveAsTable("test." + table2)


oracle_tables=["GEO_LOCATION", "CC_TRANS", "NEWS_MEDIA", "SOCIAL_MEDIA", "WEBSITE_TRAFFIC"]
for tab in oracle_tables:

    table1 = tab
    table = "SELECT * FROM test." + table1 + "a"
    table2 = table1 + "b"

    df = spark.sql(table)
    df.write.format("orc").saveAsTable("test." + table2)

postgres_tables=["postgrestable"]
for tab in postgres_tables:

    table1 = tab
    table = "SELECT * FROM test." + table1 + "a"
    table2 = table1 + "b"

    df = spark.sql(table)
    df.write.format("orc").saveAsTable("test." + table2)
