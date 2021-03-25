import os
from pyspark.sql import SparkSession

from pyspark import SparkConf, SQLContext, HiveContext, SparkContext

os.environ["SPARK_HOME"] = "/usr/local/hadoop-ecosystem/spark-3.0.1-bin"

spark = SparkSession.builder.appName("Mysql_Spark_DF").master("local[*]").enableHiveSupport().config("spark.jars","/usr/local/hadoop-ecosystem/spark-3.0.1-bin/jars/mysql-connector-java-5.1.48.jar").getOrCreate()

sc = spark.sparkContext


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

rdbms_df.show()
rdbms_df.createOrReplaceTempView("temp_tab")
df =spark.sql("select * from temp_tab")
#df.write.format("orc").mode("overwrite").saveAsTable("OHLC_prices20")
df.write.format("orc").mode("overwrite").save("/user/hive/external/OHLC_prices20")

table1="OHLC_prices29"
table="SELECT * FROM test."+table1+"a"
table2=table1+"b"

df.write.option('path', '/user/hive/external/OHLC_prices20').saveAsTable("test." + table1 + "a")

#table=
#df = spark.sql("""SELECT * FROM test.OHLC_prices27a""")

df = spark.sql(table)
df.write.format("orc").saveAsTable("test."+table2)

#df = spark.sql("SELECT * FROM test.OHLC_prices27a")
#df.write.format("orc").saveAsTable("test.OHLC_prices27b")








