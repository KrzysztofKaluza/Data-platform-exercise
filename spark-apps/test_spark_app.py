from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("PySpark-MinIO-AWS-v2")
    # Konfiguracja połączenia z MinIO
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# 1. Odczyt danych z lokalnych ścieżek
bikes_df = spark.read.options(header=True, inferSchema=True).csv("/opt/data/bikes.csv")
rentals_df = spark.read.options(header=True, inferSchema=True).csv("/opt/data/rentals.csv")
stations_df = spark.read.options(header=True, inferSchema=True).csv("/opt/data/stations.csv")

# 2. Transformacje
joined_df = rentals_df.alias("rentals").join(
    bikes_df.alias("bikes"), 
    F.col("rentals.bike_id") == F.col("bikes.bike_id"), 
    "left"
)

result_df = joined_df.select("rentals.*", "bikes.bike_type")

# Agregacja w celach podglądowych
agg_df = result_df.groupBy(F.col("bike_type")).agg(F.count("*").alias("total_rentals"))
agg_df.show()

# 3. Zapis wyniku do MinIO (S3)
result_df.write.mode("overwrite").parquet(
    "s3a://gold/przeprocesowane_dane/joined_data.parquet"
)

spark.stop()