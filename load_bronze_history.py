import os
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = (
    SparkSession.builder
    .appName("snapshot-core-banking")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", "file:/home/iceberg/warehouse")
    .config("spark.sql.defaultCatalog", "lab")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)



tables = ("customers","accounts","transactions","gl_accounts","journal_entries","journal_postings")
snapshot_id = str(uuid4())
snapshot_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

try:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.bronze_history")

    for table in tables:
        target = f"lab.bronze_history.{table}"

        source = (
            spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/iceberg_lab")
            .option("dbtable", f"source.{table}")
            .option("user", os.environ["PGUSER"])
            .option("password", os.environ["PGPASSWORD"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        snapshot = (
            source
            .withColumn("_source_system", lit("core_banking"))
            .withColumn("_snapshot_id", lit(snapshot_id))
            .withColumn("_snapshot_at", lit(snapshot_at).cast("timestamp"))
        )

        if spark.catalog.tableExists(target):
            snapshot.writeTo(target).append()
        else:
            snapshot.writeTo(target).using("iceberg").create()

        print(f"{target}: snapshot_id={snapshot_id}, rows={source.count()}")

finally:
    spark.stop()