import os
from pyspark.sql import SparkSession

from pyspark import SparkConf, SQLContext, HiveContext, SparkContext

os.environ["SPARK_HOME"] = "/usr/local/hadoop-ecosystem/spark-3.0.1-bin"
spark = SparkSession.builder.appName("Mysql_Spark_DF").master("local[*]").enableHiveSupport().config("spark.jars", "/usr/local/hadoop-ecosystem/spark-3.0.1-bin/jars/mysql-connector-java-5.1.48.jar").getOrCreate()
spark1 = SparkSession.builder.appName("Mysql_Spark_DF").master("local[*]").enableHiveSupport().config("spark.jars", "/usr/local/hadoop-ecosystem/spark-3.0.1-bin/jars/ojdbc6_g.jar").getOrCreate()
spark2 = SparkSession.builder.appName("Mysql_Spark_DF").master("local[*]").enableHiveSupport().config("spark.jars", "/usr/local/hadoop-ecosystem/spark-3.0.1-bin/jars/postgresql-42.2.18.jar").getOrCreate()


mysql_tables=["OHLC_prices", "email_receipts", "financial_statements", "human_capital", "HM"]
for tab in mysql_tables:

     rdbms_df = spark.read \
        .format("jdbc") \
        .option('url', 'jdbc:mysql://localhost:3306/STOCK_TRADE') \
        .option('user', 'root') \
        .option('password', 'root') \
        .option('dbtable', tab) \
        .load()

     rdbms_df.show()
     rdbms_df.createOrReplaceTempView("temp_tab")
     df = spark.sql("select * from temp_tab")
     df.write.format("orc").mode("overwrite").save("/user/hive/external/"+tab)

     table1 = tab
     table = "SELECT * FROM test." + table1 + "a"
     table2 = table1 + "b"

     df.write.option('path', '/user/hive/external/'+tab).saveAsTable("test." + table1 + "a")
     #df = spark.sql(table)
     #df.write.format("orc").saveAsTable("test." + table2)

oracle_tables=["GEO_LOCATION", "CC_TRANS", "NEWS_MEDIA", "SOCIAL_MEDIA", "WEBSITE_TRAFFIC"]
for tab in oracle_tables:

     rdbms_df = spark1.read \
        .format("jdbc") \
        .option('url', 'jdbc:oracle:thin:@localhost:1521/xe') \
        .option('user', 'system') \
        .option('password', 'oracle') \
        .option('dbtable', tab) \
        .load()

     rdbms_df.show()
     rdbms_df.createOrReplaceTempView("temp_tab")
     df = spark.sql("select * from temp_tab")
     df.write.format("orc").mode("overwrite").save('/user/hive/external/'+tab)

     table1 = tab
     table = "SELECT * FROM test." + table1 + "a"
     table2 = table1 + "b"

     df.write.option('path', '/user/hive/external/'+tab).saveAsTable("test." + table1 + "a")
     #df = spark.sql(table)
     #df.write.format("orc").saveAsTable("test." + table2)

postgres_tables=["postgrestable"]
for tab in postgres_tables:

     rdbms_df = spark2.read \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:5432/postgres') \
        .option('user', 'postgres') \
        .option('password', 'password') \
        .option('dbtable', tab) \
        .load()

     rdbms_df.show()
     rdbms_df.createOrReplaceTempView("temp_tab")
     df = spark.sql("select * from temp_tab")
     df.write.format("orc").mode("overwrite").save('/user/hive/external/'+tab)

     table1 = tab
     table = "SELECT * FROM test." + table1 + "a"
     table2 = table1 + "b"

     df.write.option('path', '/user/hive/external/'+tab).saveAsTable("test." + table1 + "a")
     #df = spark.sql(table)
     #df.write.format("orc").saveAsTable("test." + table2)
