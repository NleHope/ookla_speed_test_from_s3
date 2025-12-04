from pyspark.sql import SparkSession
from helpers import load_cfg

CFG_FILE = "./utils/config.yaml"

def main():
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    fake_data_cfg = cfg["fake_data"]

    # Create Spark session with MinIO/S3 configuration
    spark = SparkSession.builder \
        .appName("IngestToMinIO") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{datalake_cfg['endpoint']}") \
        .config("spark.hadoop.fs.s3a.access.key", datalake_cfg["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", datalake_cfg["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Read parquet files
    df = spark.read.parquet(f"{fake_data_cfg['folder_path']}/*.parquet")
    
    # Write to MinIO
    df.write.mode("overwrite").parquet(
        f"s3a://{datalake_cfg['bucket_name']}/{datalake_cfg['folder_name']}"
    )
    
    spark.stop()

if __name__ == "__main__":
    main()