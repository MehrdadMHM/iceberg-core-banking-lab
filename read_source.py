import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check-postgres-source").getOrCreate()

transactions = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/iceberg_lab")
    .option("dbtable", "source.transactions")
    .option("user", os.environ["PGUSER"])
    .option("password", os.environ["PGPASSWORD"])
    .option("driver", "org.postgresql.Driver")
    .load()
)

print(f"source.transactions: {transactions.count()} rows")
spark.stop()