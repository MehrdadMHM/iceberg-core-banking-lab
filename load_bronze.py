import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = (
    SparkSession.builder
    .appName("load-bronze-core-banking")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", "file:/home/iceberg/warehouse")
    .config("spark.sql.defaultCatalog", "lab")
    .getOrCreate()
)

tables = [
    "customers",
    "accounts",
    "transactions",
    "gl_accounts",
    "journal_entries",
    "journal_postings",
]

try:
    for table in tables:
        source = f"source.{table}"
        target = f"lab.bronze.{table}"

        data = (
            spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/iceberg_lab")
            .option("dbtable", source)
            .option("user", os.environ["PGUSER"])
            .option("password", os.environ["PGPASSWORD"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        bronze = (
            data
            .withColumn("_source_system", lit("core_banking"))
            .withColumn("_ingested_at", current_timestamp())
        )

        bronze.writeTo(target).using("iceberg").createOrReplace()
        print(f"{source} -> {target}: {spark.table(target).count()} rows")
finally:
    spark.stop()