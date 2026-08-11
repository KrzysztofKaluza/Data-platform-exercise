from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def prepare_spark_session():
    return (
        SparkSession.builder.appName("Silver")
        # --- Konfiguracja S3 / MinIO ---
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # --- Rozszerzenia Iceberg ---
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # --- Wspólny Katalog Iceberg (JDBC / PostgreSQL) ---
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "jdbc")
        .config(
            "spark.sql.catalog.iceberg.uri", "jdbc:postgresql://postgres:5432/warehouse"
        )
        .config("spark.sql.catalog.iceberg.jdbc.user", "admin")
        .config("spark.sql.catalog.iceberg.jdbc.password", "admin")
        # Domyślny warehouse (wymagany przez SparkCatalog, ale właściwe ścieżki i tak kontroluje metastore)
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://silver")
        .getOrCreate()
    )


def save_dataframe(df, full_table_name):
    if not spark.catalog.tableExists(full_table_name):
        df.writeTo(full_table_name).tableProperty("format-version", "2").create()
    else:
        df.writeTo(full_table_name).append()


# def create_rentals_enriched():


def prepare_silver_namespace(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.silver.etl_checkpoints (
            table_name STRING,
            last_processed_snapshot_id LONG,
            updated_at TIMESTAMP
        )
        USING iceberg
    """)


def get_last_tables_snapshot_ids(spark):
    checkpoint_rentals_rows = spark.table("iceberg.silver.etl_checkpoints").collect()
    return {
        x["table_name"]: x["last_processed_snapshot_id"]
        for x in checkpoint_rentals_rows
    }


def get_latest_tables_snapshot_ids(spark):
    tables_df = spark.sql("SHOW TABLES IN iceberg.bronze")
    table_names = [row.tableName for row in tables_df.collect()]

    union_query = " UNION ALL ".join(
        [
            f"(SELECT 'iceberg.bronze.{table}' AS table_name, snapshot_id, committed_at FROM iceberg.bronze.{table}.snapshots ORDER BY committed_at DESC LIMIT 1)"
            for table in table_names
        ]
    )

    latest_snapshots_df = spark.sql(union_query)

    return {x["table_name"]: x["snapshot_id"] for x in latest_snapshots_df.collect()}


def read_dimension_tables(spark):
    dimension_tables = ["bikes", "stations", "customers"]
    return {name: spark.table(f"iceberg.bronze.{name}") for name in dimension_tables}


def create_or_replace_dimension_tables(dimension_dfs):
    for name, df in dimension_dfs.items():
        df.writeTo(f"iceberg.silver.{name}").tableProperty(
            "format-version", "2"
        ).createOrReplace()


def create_or_append_rentals_enriched_err_tables(
    spark, latest_rentals_snapshot_id, last_rentals_snapshot_id, dimension_dfs
):

    if not latest_rentals_snapshot_id:
        print("Tabela iceberg.bronze.rentals jest pusta. Przerwanie procesu.")
        return

    if last_rentals_snapshot_id == latest_rentals_snapshot_id:
        print("Brak nowych danych w iceberg.bronze.rentals do przetworzenia.")
        return

    if last_rentals_snapshot_id is None:
        print(
            "Pierwsze uruchomienie. Wczytywanie całej tabeli iceberg.bronze.rentals..."
        )
        rental_df = spark.table("iceberg.bronze.rentals")
    else:
        print(
            f"Wczytywanie przyrostowe z iceberg.bronze.rentals (od snapshotu {last_rentals_snapshot_id} do {latest_rentals_snapshot_id})..."
        )
        rental_df = (
            spark.read.option("start-snapshot-id", last_rentals_snapshot_id)
            .option("end-snapshot-id", latest_rentals_snapshot_id)
            .table("iceberg.bronze.rentals")
        )

    rentals_enriched_df = (
        rental_df.withColumn(
            "rental_duration_minutes",
            F.round(
                (F.col("end_time").cast("long") - F.col("start_time").cast("long"))
                / 60,
                2,
            ),
        )
        .join(
            dimension_dfs["bikes"].select("bike_id", "bike_type"),
            on=rental_df["bike_id"] == dimension_dfs["bikes"]["bike_id"],
            how="left",
        )
        .join(
            dimension_dfs["stations"].select(
                "station_id",
                dimension_dfs["stations"]["station_name"].alias("start_station_name"),
            ),
            on=rental_df["start_station_id"] == dimension_dfs["stations"]["station_id"],
            how="left",
        )
        .drop("station_id")
        .join(
            dimension_dfs["stations"].select(
                "station_id",
                dimension_dfs["stations"]["station_name"].alias("end_station_name"),
            ),
            on=rental_df["end_station_id"] == dimension_dfs["stations"]["station_id"],
            how="left",
        )
        .drop("station_id")
        .join(
            dimension_dfs["customers"].select(
                "customer_id", dimension_dfs["customers"]["city"].alias("customer_city")
            ),
            on=rental_df["customer_id"] == dimension_dfs["customers"]["customer_id"],
            how="left",
        )
        .select(
            rental_df["rental_id"],
            rental_df["customer_id"],
            rental_df["bike_id"],
            rental_df["start_station_id"],
            rental_df["end_station_id"],
            "customer_city",
            "bike_type",
            "start_station_name",
            "end_station_name",
            rental_df["start_time"],
            rental_df["end_time"],
            "rental_duration_minutes",
        )
    )

    # 1. Tworzymy definicje testów DQ za pomocą reguł warunkowych (ARRAY z błędami)
    dq_checks = F.array_except(
        F.array(
            F.when(F.col("rental_id").isNull(), "RENTAL_ID_NULL"),
            F.when(F.col("bike_id").isNull(), "BIKE_ID_NULL"),
            F.when(F.col("customer_id").isNull(), "CUSTOMER_ID_NULL"),
            F.when(F.col("rental_duration_minutes") <= 0, "DURATION_NOT_POSITIVE"),
        ),
        F.array(F.lit(None)),
    )

    df_validated = rentals_enriched_df.withColumn("rental_errors_arr", dq_checks)

    df_errors = (
        df_validated.filter(F.size(F.col("rental_errors_arr")) > 0)
        .withColumn("rejected_at", F.current_timestamp())
        .select(
            "rental_id",
            "customer_id",
            "bike_id",
            "start_time",
            "end_time",
            "rental_duration_minutes",
            "rental_errors_arr",  # Tablica ze znalezionymi błędami, np. ["INVALID_DURATION", "UNMATCHED_BIKE_ID"]
            "rejected_at",
        )
    )
    save_dataframe(df_errors, "iceberg.silver.rentals_errors")

    df_clean = df_validated.filter(F.size(F.col("rental_errors_arr")) == 0).drop(
        F.col("rental_errors_arr")
    )
    save_dataframe(df_clean, "iceberg.silver.rentals_enriched")

    print(
        f"Aktualizacja checkpointu dla iceberg.bronze.rentals do snapshotu {latest_rentals_snapshot_id}..."
    )

    spark.sql(f"""
        MERGE INTO iceberg.silver.etl_checkpoints target
        USING (
            SELECT 'iceberg.bronze.rentals' as table_name, 
                {latest_rentals_snapshot_id} as last_processed_snapshot_id, 
                current_timestamp() as updated_at
        ) source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN
            UPDATE SET target.last_processed_snapshot_id = source.last_processed_snapshot_id,
                    target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN
            INSERT (table_name, last_processed_snapshot_id, updated_at) 
            VALUES (source.table_name, source.last_processed_snapshot_id, source.updated_at)
    """)

    print("--- SUKCES: Proces przyrostowy zakończony ---")


if __name__ == "__main__":
    spark = prepare_spark_session()
    prepare_silver_namespace(spark)
    latest_snapshot_ids = get_latest_tables_snapshot_ids(spark)
    last_snapshot_ids = get_last_tables_snapshot_ids(spark)
    dimension_dfs = read_dimension_tables(spark)
    create_or_replace_dimension_tables(dimension_dfs)
    create_or_append_rentals_enriched_err_tables(
        spark,
        latest_snapshot_ids.get("iceberg.bronze.rentals"),
        last_snapshot_ids.get("iceberg.bronze.rentals"),
        dimension_dfs,
    )
    spark.stop()
