import os
from pyspark.sql import SparkSession

from pyspark import SparkConf, SQLContext, HiveContext, SparkContext

os.environ["SPARK_HOME"] = "/usr/local/hadoop-ecosystem/spark-3.0.1-bin"
spark = SparkSession.builder.appName("Mysql_Spark_DF").master("local[*]").enableHiveSupport().config("spark.jars","/usr/local/hadoop-ecosystem/spark-3.0.1-bin/jars/mysql-connector-java-5.1.48.jar").getOrCreate()
spark1 = SparkSession.builder.appName("Mysql_Spark_DF").master("local[*]").enableHiveSupport().config("spark.jars", "/usr/local/hadoop-ecosystem/spark-3.0.1-bin/jars/ojdbc6_g.jar").getOrCreate()
spark2 = SparkSession.builder.appName("Mysql_Spark_DF").master("local[*]").enableHiveSupport().config("spark.jars", "/usr/local/hadoop-ecosystem/spark-3.0.1-bin/jars/postgresql-42.2.18.jar").getOrCreate()

mysql_tables=["OHLC_prices", "email_receipts", "financial_statements", "human_capital", "HM"]
for tab in mysql_tables:

    table1 = tab
    table = "SELECT * FROM test." + table1 + "b"
    df = spark1.sql(table)
    df.write \
        .format("jdbc") \
        .option("url", 'jdbc:mysql://localhost:3306/STOCK_TRADE') \
        .option("dbtable", tab+'_hivetable') \
        .option("user", "root") \
        .option("password", "root") \
        .save()



oracle_tables=["GEO_LOCATION", "CC_TRANS", "NEWS_MEDIA", "SOCIAL_MEDIA", "WEBSITE_TRAFFIC"]
for tab in oracle_tables:

    table1 = tab
    table = "SELECT * FROM test." + table1 + "b"
    df = spark1.sql(table)

    df.write \
        .format("jdbc") \
        .option("url", 'jdbc:oracle:thin:@localhost:1521/xe') \
        .option("dbtable", 'STOCK_TRADE.'+tab+'_hivetable') \
        .option("user", "system") \
        .option("password", "oracle") \
        .save()


postgres_tables=["postgrestable"]
for tab in postgres_tables:

    table1 = tab
    table = "SELECT * FROM test." + table1 + "b"
    df = spark2.sql(table)

    df.write \
        .format("jdbc") \
        .option("url", 'jdbc:postgresql://localhost:5432/postgres') \
        .option("dbtable", tab+'_hivetable') \
        .option("user", "postgres") \
        .option("password", "password") \
        .save()
