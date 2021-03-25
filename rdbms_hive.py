import os
from pyspark.sql import SparkSession

from pyspark import SparkConf, SQLContext, HiveContext, SparkContext

os.environ["SPARK_HOME"] = "/usr/local/hadoop-ecosystem/spark-3.0.1-bin"

spark = SparkSession.builder.appName("Mysql_Spark_DF").master("local[*]").enableHiveSupport().config("spark.jars", "/usr/local/hadoop-ecosystem/spark-3.0.1-bin/jars/mysql-connector-java-5.1.48.jar").getOrCreate()

sc = spark.sparkContext

#sqlc = SQLContext(sc)

print(sc)

# spark.set("spark.jars","/home/hadoop/spark/jars/postgresql-42.2.11.jar")


# df = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/databasename") \
#     .option("dbtable", "tablename") \
#     .option("user", "username") \
#     .option("password", "password") \
#     .option("driver", "org.postgresql.Driver") \
#     .load()
#
# df.printSchema()

rdbms_df = spark.read \
    .format("jdbc") \
    .option('url', 'jdbc:mysql://localhost:3306/STOCK_TRADE') \
    .option('user', 'root') \
    .option('password', 'root') \
    .option('dbtable', 'OHLC_prices') \
    .load()

# parq_df.show(5)

rdbms_df.show()

#rdbms_df.write.mode("OVERWRITE").option("path", "/user/hive/external/mysql/OHLC_prices").saveAsTable("OHLC_prices")




rdbms_df.write.format("orc").saveAsTable("test.rdbms_df1")



# Queries are expressed in HiveQL
#df = spark.sql("SELECT * FROM OHLC_prices")
#df.write.format("orc").saveAsTable("test.df1")
