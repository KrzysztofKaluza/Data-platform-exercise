from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("Bronze")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "jdbc")
    .config(
        "spark.sql.catalog.iceberg.uri", "jdbc:postgresql://postgres:5432/warehouse"
    )
    .config("spark.sql.catalog.iceberg.jdbc.user", "admin")
    .config("spark.sql.catalog.iceberg.jdbc.password", "admin")
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://bronze")
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")

spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.bronze.processed_files (
        file_name STRING,
        created_at TIMESTAMP
    )
    USING iceberg
""")

data_path = Path("/opt/data/")
file_type_names = ["bikes", "rentals", "stations", "customers", "maintenance"]

all_local_files = [
    (f.name, str(f))
    for f in data_path.iterdir()
    if f.is_file() and any(f.stem.startswith(t) for t in file_type_names)
]

local_files_df = spark.createDataFrame(all_local_files, ["file_name", "full_path"])

processed_df = spark.table("iceberg.bronze.processed_files")

new_files_df = local_files_df.join(
    processed_df, on="file_name", how="left_anti"
).cache()

if new_files_df.isEmpty():
    print("No new files to proces.")
else:
    for table_name in file_type_names:
        paths_to_process = [
            row.full_path
            for row in (
                new_files_df.filter(F.col("file_name").startswith(table_name))
                .select("full_path")
                .collect()
            )
        ]

        if not paths_to_process:
            continue

        print(
            f"\nLoading {len(paths_to_process)} new files for category '{table_name}'..."
        )

        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(paths_to_process)
        )

        full_table_name = f"iceberg.bronze.{table_name}"
        print(f"Saving to Iceberg table: {full_table_name}...")

        if not spark.catalog.tableExists(full_table_name):
            df.writeTo(full_table_name).tableProperty("format-version", "2").create()
        else:
            df.writeTo(full_table_name).append()

    print("\nRegistering new files in `processed_files`...")

    files_to_log = new_files_df.select("file_name").withColumn(
        "created_at", F.current_timestamp()
    )

    files_to_log.writeTo("iceberg.bronze.processed_files").append()

    new_files_df.unpersist()

print("\n--- PROCESSING END ---")

spark.sql("SHOW TABLES IN iceberg.bronze").show()

spark.stop()
